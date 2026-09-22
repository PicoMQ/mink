//! The Flight SQL front: statements and prepared statements carry their SQL as the handle, so any
//! replica can serve any ticket; catalog browsing answers from the engine's catalog.

use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
    CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, IpcMessage, SchemaAsIpc,
    Ticket,
};
use arrow_schema::Schema;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::TableType;
use futures::{Stream, TryStreamExt};
use mink_query::{Config, Engine};
use prost::Message;
use tonic::{Request, Response, Status};

type DoGet = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

#[derive(Clone)]
pub struct Service {
    engine: Arc<Engine>,
    info: Arc<SqlInfoData>,
}

impl Service {
    pub fn new(engine: Arc<Engine>) -> anyhow::Result<Self> {
        let mut info = SqlInfoDataBuilder::new();
        info.append(SqlInfo::FlightSqlServerName, "mink-query");
        info.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        info.append(SqlInfo::FlightSqlServerArrowVersion, arrow::ARROW_VERSION);
        info.append(SqlInfo::FlightSqlServerReadOnly, true);

        Ok(Service {
            engine,
            info: Arc::new(info.build()?),
        })
    }

    async fn plan(&self, sql: &str) -> Result<DataFrame, Status> {
        self.engine.sql(sql).await.map_err(internal)
    }

    fn info(
        &self,
        schema: &Schema,
        ticket: impl ProstMessageExt,
        descriptor: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = Ticket::new(ticket.as_any().encode_to_vec());
        let info = FlightInfo::new()
            .try_with_schema(schema)
            .map_err(internal)?
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    fn batch(&self, batch: RecordBatch) -> Result<Response<DoGet>, Status> {
        let schema = batch.schema();
        Ok(Response::new(encode(
            schema,
            futures::stream::once(async { Ok::<_, FlightError>(batch) }),
        )))
    }

    async fn tables(&self, command: CommandGetTables) -> Result<RecordBatch, Status> {
        self.engine.refresh().await.map_err(internal)?;
        let context = self.engine.context();
        let mut builder = command.into_builder();
        if let Some(catalog) = context.catalog(Config::CATALOG) {
            for schema_name in catalog.schema_names() {
                let Some(schema) = catalog.schema(&schema_name) else {
                    continue;
                };
                for table_name in schema.table_names() {
                    if let Some(table) = schema.table(&table_name).await.map_err(internal)? {
                        builder
                            .append(
                                Config::CATALOG,
                                &schema_name,
                                &table_name,
                                table_type(table.table_type()),
                                &table.schema(),
                            )
                            .map_err(internal)?;
                    }
                }
            }
        }

        builder.build().map_err(internal)
    }
}

#[tonic::async_trait]
impl FlightSqlService for Service {
    type FlightService = Service;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let frame = self.plan(&query.query).await?;
        let schema = frame.schema().as_arrow();
        let ticket = TicketStatementQuery {
            statement_handle: query.query.into(),
        };
        self.info(schema, ticket, request.into_inner())
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _: Request<Ticket>,
    ) -> Result<Response<DoGet>, Status> {
        let sql = handle(&ticket.statement_handle)?;
        let frame = self.plan(&sql).await?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let rows = frame.execute_stream().await.map_err(internal)?;

        Ok(Response::new(encode(schema, rows)))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let frame = self.plan(&query.query).await?;
        let schema = frame.schema().as_arrow();
        let IpcMessage(dataset_schema) = SchemaAsIpc::new(schema, &Default::default())
            .try_into()
            .map_err(internal)?;

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: query.query.into(),
            dataset_schema,
            parameter_schema: Default::default(),
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        _: ActionClosePreparedStatementRequest,
        _: Request<Action>,
    ) -> Result<(), Status> {
        Ok(())
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = handle(&query.prepared_statement_handle)?;
        let frame = self.plan(&sql).await?;
        let schema = frame.schema().as_arrow();
        self.info(schema, query, request.into_inner())
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<DoGet>, Status> {
        let ticket = TicketStatementQuery {
            statement_handle: query.prepared_statement_handle,
        };
        self.do_get_statement(ticket, request).await
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        self.info(&schema, query, request.into_inner())
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _: Request<Ticket>,
    ) -> Result<Response<DoGet>, Status> {
        let mut builder = query.into_builder();
        builder.append(Config::CATALOG);
        self.batch(builder.build().map_err(internal)?)
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        self.info(&schema, query, request.into_inner())
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _: Request<Ticket>,
    ) -> Result<Response<DoGet>, Status> {
        self.engine.refresh().await.map_err(internal)?;
        let mut builder = query.into_builder();
        if let Some(catalog) = self.engine.context().catalog(Config::CATALOG) {
            for name in catalog.schema_names() {
                builder.append(Config::CATALOG, name);
            }
        }
        self.batch(builder.build().map_err(internal)?)
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        self.info(&schema, query, request.into_inner())
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _: Request<Ticket>,
    ) -> Result<Response<DoGet>, Status> {
        let batch = self.tables(query).await?;
        self.batch(batch)
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        self.info(&schema, query, request.into_inner())
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _: Request<Ticket>,
    ) -> Result<Response<DoGet>, Status> {
        let mut builder = query.into_builder();
        builder.append(table_type(TableType::Base));
        self.batch(builder.build().map_err(internal)?)
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder(&self.info).schema();
        self.info(&schema, query, request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _: Request<Ticket>,
    ) -> Result<Response<DoGet>, Status> {
        let batch = query.into_builder(&self.info).build().map_err(internal)?;
        self.batch(batch)
    }

    async fn do_get_fallback(
        &self,
        _: Request<Ticket>,
        message: Any,
    ) -> Result<Response<DoGet>, Status> {
        Err(Status::unimplemented(format!(
            "unsupported ticket {}",
            message.type_url
        )))
    }

    async fn register_sql_info(&self, _: i32, _: &SqlInfo) {}
}

fn encode(
    schema: Arc<Schema>,
    rows: impl Stream<Item = Result<RecordBatch, impl std::error::Error + Send + Sync + 'static>>
    + Send
    + 'static,
) -> DoGet {
    let rows = rows.map_err(|e| FlightError::ExternalError(Box::new(e)));
    let frames = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(rows)
        .map_err(|e| Status::internal(e.to_string()));

    Box::pin(frames)
}

fn handle(bytes: &[u8]) -> Result<String, Status> {
    String::from_utf8(bytes.to_vec())
        .map_err(|_| Status::invalid_argument("statement handle is not UTF-8 SQL"))
}

fn table_type(kind: TableType) -> &'static str {
    match kind {
        TableType::Base => "TABLE",
        TableType::View => "VIEW",
        TableType::Temporary => "LOCAL TEMPORARY",
    }
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}
