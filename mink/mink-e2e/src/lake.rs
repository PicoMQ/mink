//! An independent view of the lake: the same Iceberg REST catalog the nodes commit to, read
//! with the iceberg crate directly and, when available, with the DuckDB CLI.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use mink_table::Path;
use tokio::process::Command;

use crate::Env;

pub struct Lake {
    catalog: Arc<dyn Catalog>,
    env: Env,
}

impl Lake {
    pub async fn connect(env: &Env) -> Lake {
        let props: HashMap<String, String> = [
            (REST_CATALOG_PROP_URI, env.iceberg_rest.as_str()),
            (REST_CATALOG_PROP_WAREHOUSE, env.warehouse.as_str()),
            ("s3.endpoint", env.s3_endpoint.as_str()),
            ("s3.region", "us-east-1"),
            ("s3.path-style-access", "true"),
            ("s3.access-key-id", "mink"),
            ("s3.secret-access-key", "minkminkmink"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
            .load("mink-e2e", props)
            .await
            .expect("connect to the Iceberg REST catalog");
        Lake {
            catalog: Arc::new(catalog),
            env: env.clone(),
        }
    }

    pub fn ident(path: &Path) -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new(path.database().to_string()),
            path.table().to_string(),
        )
    }

    pub async fn exists(&self, path: &Path) -> bool {
        self.catalog
            .table_exists(&Self::ident(path))
            .await
            .expect("table_exists")
    }

    pub async fn table(&self, path: &Path) -> iceberg::table::Table {
        self.catalog
            .load_table(&Self::ident(path))
            .await
            .unwrap_or_else(|e| panic!("load {path} from the lake: {e}"))
    }

    pub async fn current_snapshot_id(&self, path: &Path) -> Option<i64> {
        self.table(path)
            .await
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
    }

    pub async fn rows(&self, path: &Path) -> Vec<RecordBatch> {
        let table = self.table(path).await;
        if table.metadata().current_snapshot().is_none() {
            return Vec::new();
        }
        table
            .scan()
            .build()
            .expect("scan builds")
            .to_arrow()
            .await
            .expect("scan opens")
            .try_collect()
            .await
            .expect("scan reads")
    }

    pub async fn column_names(&self, path: &Path) -> Vec<String> {
        self.table(path)
            .await
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    pub async fn drop(&self, path: &Path) {
        let ident = Self::ident(path);
        if self.catalog.table_exists(&ident).await.unwrap_or(false) {
            self.catalog
                .drop_table(&ident)
                .await
                .expect("drop lake table");
        }
    }

    pub async fn duckdb_count(&self, path: &Path) -> Option<i64> {
        let sql = format!(
            "INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs; \
             CREATE SECRET s3 (TYPE s3, KEY_ID 'mink', SECRET 'minkminkmink', REGION 'us-east-1', \
             ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false); \
             ATTACH '{warehouse}' AS lake (TYPE iceberg, ENDPOINT '{rest}', AUTHORIZATION_TYPE 'none'); \
             SELECT count(*) AS n FROM lake.\"{db}\".\"{table}\";",
            endpoint = self.env.s3_endpoint.trim_start_matches("http://"),
            warehouse = self.env.warehouse,
            rest = self.env.iceberg_rest,
            db = path.database(),
            table = path.table(),
        );
        let output = Command::new("duckdb")
            .arg("-json")
            .arg("-c")
            .arg(&sql)
            .stdin(Stdio::null())
            .output()
            .await
            .ok()?;
        assert!(
            output.status.success(),
            "duckdb failed: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            sql
        );
        let rows: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(&output.stdout)
            .into_iter::<Vec<serde_json::Value>>()
            .last()
            .expect("duckdb printed no result")
            .expect("duckdb json");
        rows.first()
            .and_then(|r| r.get("n"))
            .and_then(serde_json::Value::as_i64)
    }
}
