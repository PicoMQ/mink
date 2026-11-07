mod attach;
mod auth;
mod storage;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::{DataFile, Schema, SortOrder, TableMetadata, UnboundPartitionSpec};
use iceberg::table::Table as IcebergTable;
use iceberg::{
    ErrorKind, Namespace, NamespaceIdent, Result, Runtime, TableCommit, TableCreation, TableIdent,
    TableRequirement, TableUpdate,
};
use iceberg_catalog_rest::{
    CommitTableRequest, CommitTableResponse, CreateTableRequest, ErrorResponse, LoadTableResult,
};
use mink_core::{CatalogTable, ExternalTable};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, Response, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

pub use attach::{Attachment, Partitioning, SortTerm, Source, Term};
pub(crate) use attach::{attach, detach};
use auth::Authorizer;
use storage::S3StorageFactory;

use crate::buckets::Buckets;
use crate::config::ExternalCatalog;

const TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct Catalog {
    name: String,
    http: reqwest::Client,
    uri: String,
    warehouse: Option<String>,
    authorizer: Authorizer,
    file_io: FileIO,
    runtime: Runtime,
    root: OnceCell<Url>,
}

#[derive(Deserialize)]
struct ConfigResponse {
    #[serde(default)]
    defaults: HashMap<String, String>,
    #[serde(default)]
    overrides: HashMap<String, String>,
}

impl Catalog {
    pub(crate) async fn new(
        name: &str,
        catalog: &ExternalCatalog,
        buckets: &Buckets,
    ) -> std::result::Result<Catalog, crate::Error> {
        let http = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .build()
            .map_err(crate::Error::HttpClient)?;
        let uri = catalog.uri.trim_end_matches('/').to_owned();
        let authorizer = Authorizer::new(catalog.auth.as_ref(), &uri, &http).await;
        let file_io = FileIOBuilder::new(Arc::new(S3StorageFactory::new(buckets.clone()))).build();
        let runtime = Runtime::try_current().map_err(crate::Error::from)?;

        Ok(Catalog {
            name: name.to_owned(),
            http,
            uri,
            warehouse: catalog.warehouse.clone(),
            authorizer,
            file_io,
            runtime,
            root: OnceCell::new(),
        })
    }

    pub(crate) async fn create_table_with_ids(
        &self,
        identifier: &TableIdent,
        schema: Schema,
        spec: UnboundPartitionSpec,
        order: SortOrder,
        properties: HashMap<String, String>,
        location: Option<String>,
    ) -> Result<IcebergTable> {
        let creation = TableCreation::builder()
            .name(identifier.name().to_owned())
            .location_opt(location)
            .schema(schema.clone())
            .properties(properties)
            .build();
        let created =
            iceberg::Catalog::create_table(self, identifier.namespace(), creation).await?;

        let metadata = created.metadata();
        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id(),
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: metadata.last_column_id(),
            },
        ];
        let mut updates = vec![
            TableUpdate::AddSchema { schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];
        if !spec.fields().is_empty() {
            let spec_id = metadata
                .partition_specs_iter()
                .map(|spec| spec.spec_id())
                .max();
            let spec = spec.with_spec_id(spec_id.map_or(0, |id| id + 1));

            updates.push(TableUpdate::AddSpec { spec });
            updates.push(TableUpdate::SetDefaultSpec { spec_id: -1 });
        }
        if !order.is_unsorted() {
            let order_id = metadata
                .sort_orders_iter()
                .map(|order| order.order_id)
                .max();
            let sort_order = order.with_order_id(order_id.map_or(1, |id| id + 1));

            updates.push(TableUpdate::AddSortOrder { sort_order });
            updates.push(TableUpdate::SetDefaultSortOrder { sort_order_id: -1 });
        }

        let assigned = self.commit(identifier, requirements, updates).await;
        if assigned.is_err()
            && let Err(error) = iceberg::Catalog::drop_table(self, identifier).await
        {
            tracing::warn!(catalog = %self.name, %error, "a created table whose ids were not assigned was not dropped");
        }
        assigned
    }

    pub(crate) async fn commit(
        &self,
        identifier: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<IcebergTable> {
        let request = CommitTableRequest {
            identifier: Some(identifier.clone()),
            requirements,
            updates,
        };

        let url = self.table_url(identifier).await?;
        let body = self.encode(&request)?;
        let committed: CommitTableResponse = self
            .send(Method::POST, url, Some(body), ErrorKind::TableNotFound)
            .await?;
        self.table(
            identifier.clone(),
            committed.metadata,
            Some(committed.metadata_location),
        )
    }

    fn table(
        &self,
        identifier: TableIdent,
        metadata: TableMetadata,
        location: Option<String>,
    ) -> Result<IcebergTable> {
        let builder = IcebergTable::builder()
            .identifier(identifier)
            .metadata(metadata)
            .file_io(self.file_io.clone())
            .runtime(self.runtime.clone());

        match location {
            Some(location) => builder.metadata_location(location).build(),
            None => builder.build(),
        }
    }

    async fn table_url(&self, table: &TableIdent) -> Result<Url> {
        self.url(&[
            "namespaces",
            &table.namespace().to_url_string(),
            "tables",
            table.name(),
        ])
        .await
    }

    async fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.root().await?.clone();

        url.path_segments_mut()
            .map_err(|()| self.invalid_uri(&self.uri))?
            .extend(segments);
        Ok(url)
    }

    async fn root(&self) -> Result<&Url> {
        self.root
            .get_or_try_init(|| async {
                let mut url = self.parse(&format!("{}/v1/config", self.uri))?;
                if let Some(warehouse) = &self.warehouse {
                    url.query_pairs_mut().append_pair("warehouse", warehouse);
                }

                let config: ConfigResponse = self
                    .send(Method::GET, url, None, ErrorKind::Unexpected)
                    .await?;
                let prefix = config
                    .overrides
                    .get("prefix")
                    .or_else(|| config.defaults.get("prefix"));

                match prefix {
                    Some(prefix) => self.parse(&format!("{}/v1/{prefix}", self.uri)),
                    None => self.parse(&format!("{}/v1", self.uri)),
                }
            })
            .await
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        url: Url,
        body: Option<Vec<u8>>,
        missing: ErrorKind,
    ) -> Result<T> {
        let (status, bytes) = self.request(method, url, body, missing).await?;

        serde_json::from_slice(&bytes)
            .map_err(|error| self.rejected(ErrorKind::Unexpected, status, error))
    }

    async fn request(
        &self,
        method: Method,
        url: Url,
        body: Option<Vec<u8>>,
        missing: ErrorKind,
    ) -> Result<(StatusCode, bytes::Bytes)> {
        let mut response = self.attempt(&method, &url, body.clone()).await?;
        if response.status() == StatusCode::UNAUTHORIZED && self.authorizer.renew().await {
            response = self.attempt(&method, &url, body).await?;
        }

        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| self.unavailable(error))?;

        if status.is_success() {
            return Ok((status, bytes));
        }

        let message = serde_json::from_slice::<ErrorResponse>(&bytes)
            .map(|response| iceberg::Error::from(response).message().to_owned())
            .unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
        match status {
            StatusCode::CONFLICT => {
                Err(self.rejected(ErrorKind::CatalogCommitConflicts, status, message))
            }
            StatusCode::NOT_FOUND => Err(self.rejected(missing, status, message)),
            status if status.is_server_error() => {
                Err(self.unavailable(format!("{status}: {message}")))
            }
            StatusCode::BAD_REQUEST => Err(self.rejected(ErrorKind::DataInvalid, status, message)),
            status => Err(self.rejected(ErrorKind::Unexpected, status, message)),
        }
    }

    async fn attempt(&self, method: &Method, url: &Url, body: Option<Vec<u8>>) -> Result<Response> {
        let mut builder = self.http.request(method.clone(), url.clone());
        if let Some(body) = body {
            builder = builder.header(CONTENT_TYPE, "application/json").body(body);
        }

        let mut request = builder.build().map_err(|error| {
            iceberg::Error::new(
                ErrorKind::Unexpected,
                format!(
                    "a request to external catalog {} cannot be built: {error}",
                    self.name
                ),
            )
        })?;
        self.authorizer
            .authorize(&mut request)
            .await
            .map_err(|reason| self.unavailable(reason))?;

        self.http
            .execute(request)
            .await
            .map_err(|error| self.unavailable(error))
    }

    fn parse(&self, url: &str) -> Result<Url> {
        Url::parse(url).map_err(|_| self.invalid_uri(url))
    }

    fn invalid_uri(&self, url: &str) -> iceberg::Error {
        iceberg::Error::new(
            ErrorKind::Unexpected,
            format!("external catalog {} has an invalid URI: {url}", self.name),
        )
    }

    fn encode<T: Serialize>(&self, request: &T) -> Result<Vec<u8>> {
        serde_json::to_vec(request).map_err(|error| {
            iceberg::Error::new(
                ErrorKind::Unexpected,
                format!(
                    "a request to external catalog {} cannot be encoded: {error}",
                    self.name
                ),
            )
        })
    }

    fn unavailable(&self, reason: impl fmt::Display) -> iceberg::Error {
        iceberg::Error::new(
            ErrorKind::Unexpected,
            format!("external catalog {} is unavailable: {reason}", self.name),
        )
    }

    fn rejected(
        &self,
        kind: ErrorKind,
        status: StatusCode,
        message: impl fmt::Display,
    ) -> iceberg::Error {
        iceberg::Error::new(
            kind,
            format!(
                "external catalog {} answered {status}: {message}",
                self.name
            ),
        )
    }
}

impl fmt::Debug for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Catalog")
            .field("name", &self.name)
            .field("uri", &self.uri)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl iceberg::Catalog for Catalog {
    async fn list_namespaces(&self, _: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>> {
        Err(unsupported("listing namespaces"))
    }

    async fn create_namespace(
        &self,
        _: &NamespaceIdent,
        _: HashMap<String, String>,
    ) -> Result<Namespace> {
        Err(unsupported("creating namespaces"))
    }

    async fn get_namespace(&self, _: &NamespaceIdent) -> Result<Namespace> {
        Err(unsupported("getting namespaces"))
    }

    async fn namespace_exists(&self, _: &NamespaceIdent) -> Result<bool> {
        Err(unsupported("checking namespaces"))
    }

    async fn update_namespace(&self, _: &NamespaceIdent, _: HashMap<String, String>) -> Result<()> {
        Err(unsupported("updating namespaces"))
    }

    async fn drop_namespace(&self, _: &NamespaceIdent) -> Result<()> {
        Err(unsupported("dropping namespaces"))
    }

    async fn list_tables(&self, _: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Err(unsupported("listing tables"))
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<IcebergTable> {
        let mut properties = creation.properties;
        properties.insert(
            "format-version".to_owned(),
            (creation.format_version as u8).to_string(),
        );
        let request = CreateTableRequest {
            name: creation.name.clone(),
            location: creation.location,
            schema: creation.schema,
            partition_spec: creation.partition_spec.map(|spec| match spec.spec_id() {
                Some(_) => spec,
                None => spec.with_spec_id(0),
            }),
            write_order: creation.sort_order,
            stage_create: None,
            properties,
        };

        let url = self
            .url(&["namespaces", &namespace.to_url_string(), "tables"])
            .await?;
        let body = self.encode(&request)?;
        let created: LoadTableResult = self
            .send(Method::POST, url, Some(body), ErrorKind::NamespaceNotFound)
            .await?;

        let identifier = TableIdent::new(namespace.clone(), creation.name);
        self.table(identifier, created.metadata, created.metadata_location)
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<IcebergTable> {
        let url = self.table_url(identifier).await?;

        let loaded: LoadTableResult = self
            .send(Method::GET, url, None, ErrorKind::TableNotFound)
            .await?;
        self.table(
            identifier.clone(),
            loaded.metadata,
            loaded.metadata_location,
        )
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        let url = self.table_url(identifier).await?;

        self.request(Method::DELETE, url, None, ErrorKind::TableNotFound)
            .await
            .map(|_| ())
    }

    async fn purge_table(&self, _: &TableIdent) -> Result<()> {
        Err(unsupported("purging tables"))
    }

    async fn table_exists(&self, _: &TableIdent) -> Result<bool> {
        Err(unsupported("checking tables"))
    }

    async fn rename_table(&self, _: &TableIdent, _: &TableIdent) -> Result<()> {
        Err(unsupported("renaming tables"))
    }

    async fn register_table(&self, _: &TableIdent, _: String) -> Result<IcebergTable> {
        Err(unsupported("registering tables"))
    }

    async fn update_table(&self, mut commit: TableCommit) -> Result<IcebergTable> {
        let identifier = commit.identifier().clone();
        let requirements = commit.take_requirements();

        self.commit(&identifier, requirements, commit.take_updates())
            .await
    }
}

pub(crate) fn identifier(table: &CatalogTable) -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new(table.namespace().to_owned()),
        table.name().to_owned(),
    )
}

pub(crate) async fn delete_data_files(table: &IcebergTable, files: &[DataFile]) {
    for file in files {
        if let Err(error) = table.file_io().delete(file.file_path()).await {
            tracing::warn!(path = file.file_path(), %error, "a tiered data file was not deleted");
        }
    }
}

pub(crate) struct Pinned<'a> {
    catalog: &'a Catalog,
    table: IcebergTable,
}

impl Catalog {
    pub(crate) async fn load(
        &self,
        external: &ExternalTable,
    ) -> std::result::Result<IcebergTable, crate::Error> {
        let table = iceberg::Catalog::load_table(self, &identifier(external.table())).await?;
        if table.metadata().uuid() != external.uuid() {
            return Err(crate::Error::ReplacedExternalTable(
                external.topic().to_owned(),
            ));
        }

        Ok(table)
    }

    pub(crate) fn pinned(&self, table: IcebergTable) -> Pinned<'_> {
        Pinned {
            catalog: self,
            table,
        }
    }
}

impl fmt::Debug for Pinned<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pinned")
            .field("catalog", self.catalog)
            .field("table", self.table.identifier())
            .finish()
    }
}

#[async_trait]
impl iceberg::Catalog for Pinned<'_> {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.catalog.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.catalog.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.catalog.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.catalog.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.catalog.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.catalog.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.catalog.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<IcebergTable> {
        self.catalog.create_table(namespace, creation).await
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<IcebergTable> {
        match identifier == self.table.identifier() {
            true => Ok(self.table.clone()),
            false => self.catalog.load_table(identifier).await,
        }
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        self.catalog.drop_table(identifier).await
    }

    async fn purge_table(&self, identifier: &TableIdent) -> Result<()> {
        self.catalog.purge_table(identifier).await
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        self.catalog.table_exists(identifier).await
    }

    async fn rename_table(&self, from: &TableIdent, to: &TableIdent) -> Result<()> {
        self.catalog.rename_table(from, to).await
    }

    async fn register_table(
        &self,
        identifier: &TableIdent,
        location: String,
    ) -> Result<IcebergTable> {
        self.catalog.register_table(identifier, location).await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<IcebergTable> {
        self.catalog.update_table(commit).await
    }
}

fn unsupported(operation: &str) -> iceberg::Error {
    iceberg::Error::new(
        ErrorKind::FeatureUnsupported,
        format!("{operation} is not part of Mink's external catalog client"),
    )
}

#[cfg(test)]
mod tests {
    use iceberg::spec::UnboundPartitionSpec;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog as _, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
    use serde_json::json;
    use uuid::Uuid;

    use super::Catalog;
    use crate::buckets::Buckets;
    use crate::config::{ExternalCatalog, ExternalCatalogAuth};
    use crate::testing::storage;

    async fn fixture(auth: Option<ExternalCatalogAuth>) -> Option<Catalog> {
        let uri = mink_conformance::service("TEST_ICEBERG_REST")?;
        let endpoint = mink_conformance::service("TEST_S3_ENDPOINT")?;
        let config = ExternalCatalog {
            uri,
            warehouse: None,
            auth,
        };

        Some(
            Catalog::new("fixture", &config, &Buckets::new(storage(endpoint)))
                .await
                .unwrap(),
        )
    }

    async fn created(catalog: &Catalog) -> TableIdent {
        let namespace = NamespaceIdent::new("mink".to_owned());
        let name = format!("t{}", Uuid::new_v4().simple());
        let creation = TableCreation::builder()
            .name(name.clone())
            .schema(mink_schema::binary_schema().clone())
            .partition_spec(UnboundPartitionSpec::builder().with_spec_id(0).build())
            .build();

        reqwest::Client::new()
            .post(format!("{}/v1/namespaces", catalog.uri))
            .json(&json!({ "namespace": ["mink"] }))
            .send()
            .await
            .unwrap();
        catalog.create_table(&namespace, creation).await.unwrap();
        TableIdent::new(namespace, name)
    }

    async fn set(catalog: &Catalog, table: &TableIdent, key: &str, value: &str) {
        let loaded = catalog.load_table(table).await.unwrap();
        let transaction = Transaction::new(&loaded);

        let action = transaction
            .update_table_properties()
            .set(key.to_owned(), value.to_owned());
        let transaction = action.apply(transaction).unwrap();
        transaction.commit(catalog).await.unwrap();
    }

    #[tokio::test]
    async fn tables_are_created_loaded_and_committed() {
        let Some(catalog) = fixture(None).await else {
            return;
        };
        let table = created(&catalog).await;

        set(&catalog, &table, "mink.partition.0.end-offset", "10").await;

        let reloaded = catalog.load_table(&table).await.unwrap();
        assert_eq!(
            reloaded.metadata().properties()["mink.partition.0.end-offset"],
            "10"
        );
    }

    #[tokio::test]
    async fn a_transaction_from_a_stale_table_lands_on_top_of_newer_commits() {
        let Some(catalog) = fixture(None).await else {
            return;
        };
        let table = created(&catalog).await;
        let stale = catalog.load_table(&table).await.unwrap();

        set(&catalog, &table, "first", "1").await;
        let transaction = Transaction::new(&stale);
        let action = transaction
            .update_table_properties()
            .set("second".to_owned(), "2".to_owned());
        let transaction = action.apply(transaction).unwrap();
        transaction.commit(&catalog).await.unwrap();

        let properties = catalog.load_table(&table).await.unwrap();
        let properties = properties.metadata().properties();
        assert_eq!(properties["first"], "1");
        assert_eq!(properties["second"], "2");
    }

    #[tokio::test]
    async fn oauth2_clients_fetch_a_token_before_their_requests() {
        let auth = ExternalCatalogAuth::Oauth2 {
            credential: "mink:secret".to_owned(),
            scope: Some("catalog".to_owned()),
            token_uri: None,
        };
        let Some(catalog) = fixture(Some(auth)).await else {
            return;
        };

        let table = created(&catalog).await;

        catalog.load_table(&table).await.unwrap();
    }

    #[tokio::test]
    async fn an_unreachable_catalog_is_not_retried() {
        let config = ExternalCatalog {
            uri: "http://127.0.0.1:1".to_owned(),
            warehouse: None,
            auth: None,
        };
        let buckets = Buckets::new(storage("http://127.0.0.1:1".to_owned()));
        let unreachable = Catalog::new("unreachable", &config, &buckets)
            .await
            .unwrap();

        let table = TableIdent::from_strs(["mink", "t"]).unwrap();
        let error = unreachable.load_table(&table).await.unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(!error.retryable(), "{error}");
    }
}
