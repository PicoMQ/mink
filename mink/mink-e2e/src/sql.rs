//! A Flight SQL client against the query service: statements, catalog browsing and typed row helpers.

use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTables};
use arrow_flight::{FlightInfo, Ticket};
use futures::TryStreamExt;
use tonic::transport::Channel;

use crate::Env;

pub struct Sql {
    client: FlightSqlServiceClient<Channel>,
}

impl Sql {
    pub async fn connect(env: &Env) -> Sql {
        let address = env.query.first().expect("MINK_E2E_QUERY is set");
        let endpoint = address.replacen("grpc://", "http://", 1);
        let channel = crate::wait_for("query service", async || {
            match Channel::from_shared(endpoint.clone())
                .expect("query address")
                .connect()
                .await
            {
                Ok(channel) => Ok(Some(channel)),
                Err(_) => Ok(None),
            }
        })
        .await;

        Sql {
            client: FlightSqlServiceClient::new(channel),
        }
    }

    pub async fn try_query(&mut self, sql: &str) -> Result<Vec<RecordBatch>, FlightError> {
        let info = self.client.execute(sql.to_owned(), None).await?;
        self.fetch(info).await
    }

    pub async fn query(&mut self, sql: &str) -> Vec<RecordBatch> {
        self.try_query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    pub async fn timed(&mut self, sql: &str) -> (Vec<RecordBatch>, Duration) {
        let started = Instant::now();
        let batches = self.query(sql).await;
        (batches, started.elapsed())
    }

    pub async fn count(&mut self, sql: &str) -> i64 {
        let batches = self.query(sql).await;
        batches[0].column(0).as_primitive::<Int64Type>().value(0)
    }

    pub async fn keyed(&mut self, sql: &str) -> Vec<(i64, String)> {
        self.query(sql)
            .await
            .iter()
            .flat_map(|b| {
                let k = b.column(0).as_primitive::<Int64Type>();
                let v = b.column(1).as_string::<i32>();
                (0..b.num_rows())
                    .map(|i| (k.value(i), v.value(i).to_owned()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub async fn strings(&mut self, sql: &str) -> Vec<String> {
        strings(&self.query(sql).await, 0)
    }

    pub async fn schemas(&mut self) -> Vec<String> {
        let info = self
            .client
            .get_db_schemas(CommandGetDbSchemas {
                catalog: Some("mink".into()),
                db_schema_filter_pattern: None,
            })
            .await
            .expect("get_db_schemas");
        strings(&self.fetch(info).await.expect("schemas"), 1)
    }

    pub async fn tables(&mut self, database: &str) -> Vec<String> {
        let info = self
            .client
            .get_tables(CommandGetTables {
                catalog: Some("mink".into()),
                db_schema_filter_pattern: Some(database.into()),
                table_name_filter_pattern: None,
                table_types: Vec::new(),
                include_schema: true,
            })
            .await
            .expect("get_tables");
        strings(&self.fetch(info).await.expect("tables"), 2)
    }

    async fn fetch(&mut self, info: FlightInfo) -> Result<Vec<RecordBatch>, FlightError> {
        let mut batches = Vec::new();
        for endpoint in info.endpoint {
            let ticket: Ticket = endpoint.ticket.expect("endpoint ticket");
            let stream = self.client.do_get(ticket).await?;
            batches.extend(stream.try_collect::<Vec<_>>().await?);
        }

        Ok(batches)
    }
}

pub fn strings(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let s = b.column(column).as_string::<i32>();
            (0..b.num_rows())
                .map(|i| s.value(i).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

pub fn percentile(samples: &mut [Duration], p: f64) -> Duration {
    samples.sort_unstable();
    let rank = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[rank]
}
