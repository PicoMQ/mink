//! The cluster's databases and tables as a DataFusion catalog, listed once per statement.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result;
use mink_client::Cluster;
use mink_common::sync::{read, write};
use mink_lake::Reader;
use mink_table::{Name, Path};
use tonic::Code;

use crate::error::Error;
use crate::provider::MinkTable;

pub struct Catalog {
    cluster: Cluster,
    lake: Option<Arc<dyn Reader>>,
    databases: RwLock<BTreeMap<String, Arc<Database>>>,
}

pub struct Database {
    name: String,
    cluster: Cluster,
    lake: Option<Arc<dyn Reader>>,
    tables: Vec<String>,
}

impl Catalog {
    pub fn new(cluster: Cluster, lake: Option<Arc<dyn Reader>>) -> Self {
        Catalog {
            cluster,
            lake,
            databases: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn refresh(&self) -> Result<(), Error> {
        let admin = self.cluster.admin();
        let names = admin.list_databases().await?;
        let listed = names.iter().map(|name| admin.list_tables(name));
        let tables = futures::future::try_join_all(listed).await?;
        let databases = names
            .into_iter()
            .zip(tables)
            .map(|(name, mut tables)| {
                tables.sort_unstable();
                let database = Database {
                    name: name.clone(),
                    cluster: self.cluster.clone(),
                    lake: self.lake.clone(),
                    tables,
                };
                (name, Arc::new(database))
            })
            .collect();
        *write(&self.databases) = databases;

        Ok(())
    }

    fn databases(&self) -> BTreeMap<String, Arc<Database>> {
        read(&self.databases).clone()
    }
}

impl fmt::Debug for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Catalog")
            .field("databases", &self.databases().keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CatalogProvider for Catalog {
    fn schema_names(&self) -> Vec<String> {
        self.databases().into_keys().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let database = self.databases().get(name)?.clone();
        Some(database)
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("name", &self.name)
            .field("tables", &self.tables)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for Database {
    fn table_names(&self) -> Vec<String> {
        self.tables.clone()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if !self.table_exist(name) {
            return Ok(None);
        }
        let path = Path::new(
            Name::new(&self.name).map_err(client)?,
            Name::new(name).map_err(client)?,
        );
        match MinkTable::open(&self.cluster, self.lake.clone(), &path).await {
            Ok(table) => Ok(Some(Arc::new(table))),
            Err(Error::Client(mink_client::Error::Status(status)))
                if status.code() == Code::NotFound =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .binary_search_by(|t| t.as_str().cmp(name))
            .is_ok()
    }
}

fn client(error: mink_table::Error) -> Error {
    Error::Client(error.into())
}
