mod cluster;
mod console;
mod files;
mod leases;
mod nodes;
mod overview;
mod paging;
mod partitions;
mod records;
mod registry;
mod reply;
mod schemas;
mod snapshots;
mod tables;
mod values;

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use mink_metrics::Metrics;
use serde_json::json;

use crate::Error;
use crate::forward::{Forwarder, Wait};
use crate::s3::Cache;
use crate::services::Services;

const ANSWER_TIMEOUT: Duration = Duration::from_secs(10);
const PROMETHEUS_TEXT: &str = "text/plain; version=0.0.4";

pub(crate) struct Admin {
    services: Arc<Services>,
    cache: Option<Arc<Cache>>,
    sql: bool,
    forwarder: Forwarder,
    overview: overview::Computed,
}

impl Admin {
    fn render(&self) -> String {
        if let Some(cache) = &self.cache {
            self.services.metrics.cache_size(cache.usage().memory_bytes);
        }

        self.services.metrics.render()
    }
}

pub(crate) fn router(
    services: Arc<Services>,
    cache: Option<Arc<Cache>>,
    catalog_and_s3: Option<(Router, Router)>,
) -> Result<Router, Error> {
    let forwarder = Forwarder::new(services.membership.id(), Wait::Whole(ANSWER_TIMEOUT))?;
    let metrics = services.metrics.clone();
    let admin = Arc::new(Admin {
        services,
        cache,
        sql: catalog_and_s3.is_some(),
        forwarder,
        overview: overview::Computed::default(),
    });

    let router = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_text))
        .merge(cluster::routes())
        .merge(overview::routes())
        .merge(nodes::routes())
        .merge(leases::routes())
        .merge(tables::routes())
        .merge(partitions::routes())
        .merge(files::routes())
        .merge(snapshots::routes())
        .merge(schemas::routes())
        .merge(records::routes())
        .merge(registry::routes())
        .fallback(console::serve)
        .layer(middleware::from_fn_with_state(metrics, counted))
        .with_state(admin);

    Ok(match catalog_and_s3 {
        Some((catalog, s3)) => router.nest("/catalog", catalog).nest("/s3", s3),
        None => router,
    })
}

pub(super) async fn lag(services: &Services, name: &str) -> Result<i64, Error> {
    let ranges = services.store.indexed_ranges().list(name).await?;
    let mut cluster = services.kafka.cluster().await?;

    let mut lag = 0;
    for range in &ranges {
        let stable = cluster.last_stable_offset(range.partition()).await?;
        lag += (stable - range.end_offset()).max(0);
    }

    Ok(lag)
}

async fn health() -> &'static str {
    "ok"
}

async fn ready(State(admin): State<Arc<Admin>>) -> Response {
    let services = &admin.services;
    let roles: Vec<&str> = services
        .config
        .roles
        .iter()
        .map(|role| role.name())
        .collect();

    let ready = services.store.members().live().await.is_ok();

    let status = match ready {
        true => StatusCode::OK,
        false => StatusCode::SERVICE_UNAVAILABLE,
    };
    let body = json!({
        "ready": ready,
        "nodeId": services.membership.id(),
        "roles": roles,
    });
    (status, Json(body)).into_response()
}

async fn metrics_text(State(admin): State<Arc<Admin>>) -> Response {
    let text = admin.render();

    ([(header::CONTENT_TYPE, PROMETHEUS_TEXT)], text).into_response()
}

async fn counted(State(metrics): State<Metrics>, request: Request, next: Next) -> Response {
    let console = console::is_console(request.uri().path());

    let response = next.run(request).await;

    if !console {
        metrics.admin_request(response.status().as_u16());
    }
    response
}

#[cfg(test)]
pub(super) mod tests {
    use bytes::Bytes;
    use mink_core::{Offsets, Part, Partition, Topic};
    use reqwest::{Method, StatusCode};
    use serde_json::{Value, json};

    use crate::testing;
    use crate::{Config, Node, Role, SchemaRegistry};

    pub(in crate::admin) async fn admin(
        roles: &[Role],
        change: impl FnOnce(&mut Config),
    ) -> Option<(Node, String)> {
        let mut config = testing::config(roles).await?;
        change(&mut config);

        let node = testing::node_with(config).await;
        let base = format!("http://{}", node.admin_address().unwrap());
        Some((node, base))
    }

    pub(in crate::admin) async fn get(base: &str, path: &str) -> (StatusCode, Value) {
        send(Method::GET, base, path, None).await
    }

    pub(in crate::admin) async fn send(
        method: Method,
        base: &str,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = reqwest::Client::new().request(method, format!("{base}{path}"));
        if let Some(body) = body {
            request = request.json(&body);
        }

        let response = request.send().await.unwrap();
        let status = response.status();
        let body = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    pub(in crate::admin) struct Registered {
        pub(in crate::admin) node: Node,
        pub(in crate::admin) base: String,
        pub(in crate::admin) topic: Topic,
        pub(in crate::admin) registry: String,
        pub(in crate::admin) bootstrap: String,
    }

    pub(in crate::admin) async fn with_registry() -> Option<Registered> {
        let registry = mink_conformance::service("TEST_SCHEMA_REGISTRY")?;
        let bootstrap = testing::kafka()?;
        let settings = SchemaRegistry {
            url: registry.clone(),
            username: None,
            password: None,
        };
        let (node, base) = admin(&[Role::Admin], |config| {
            config.schema_registry = Some(settings)
        })
        .await?;

        let topic = testing::registered(&node.services, 1).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
        Some(Registered {
            node,
            base,
            topic,
            registry,
            bootstrap,
        })
    }

    impl Registered {
        pub(in crate::admin) async fn version(&self, fields: Value) -> i32 {
            let subject = mink_schema::subject(self.topic.name(), Part::Value);
            let schema = json!({ "type": "record", "name": "V", "fields": fields });

            mink_conformance::register_schema(
                &self.registry,
                &subject,
                &json!({ "schema": schema.to_string() }),
            )
            .await
        }

        pub(in crate::admin) fn produce(&self, records: &[Vec<u8>]) {
            let records: Vec<&[u8]> = records.iter().map(Vec::as_slice).collect();

            mink_conformance::produce_bytes(&self.bootstrap, self.topic.name(), 0, &records);
        }

        pub(in crate::admin) async fn merge(&self) {
            let services = &self.node.services;
            let partition = Partition::new(self.topic.name(), 0).unwrap();
            let mut cluster = services.kafka.cluster().await.unwrap();
            let stable = cluster.last_stable_offset(&partition).await.unwrap();
            let records = cluster
                .read(&Offsets::new(partition, 0, stable).unwrap())
                .await
                .unwrap();

            services
                .schemas
                .latest_file_schema(self.topic.name(), &records)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn probes_metrics_and_unknown_routes_answer() {
        let Some((node, base)) = admin(&[Role::Admin], |_| {}).await else {
            return;
        };

        let health = reqwest::get(format!("{base}/health")).await.unwrap();
        let (ready, readiness) = get(&base, "/ready").await;
        let (missing, error) = get(&base, "/admin/nothing").await;
        let metrics = reqwest::get(format!("{base}/metrics")).await.unwrap();
        let text = metrics.text().await.unwrap();

        assert_eq!(health.text().await.unwrap(), "ok");
        assert_eq!(ready, StatusCode::OK);
        assert_eq!(readiness["ready"], true);
        assert_eq!(missing, StatusCode::NOT_FOUND);
        assert!(error["error"].is_string());
        assert!(
            text.contains(r#"mink_admin_requests_total{status="4xx"} 1"#),
            "{text}"
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn the_admin_listener_serves_the_catalog_and_s3() {
        let Some((node, base)) = admin(&[Role::Admin, Role::Catalog, Role::S3], |_| {}).await
        else {
            return;
        };
        let topic = testing::registered(&node.services, 1).await;
        let path = format!("warehouse/{}/metadata/v1-e1.metadata.json", topic.name());
        node.services
            .warehouse
            .write(&format!("s3://mink/{path}"), Bytes::from_static(b"{}"))
            .await
            .unwrap();

        let (config, _) = get(&base, "/catalog/v1/config").await;
        let (tables, _) = get(&base, "/catalog/v1/namespaces/kafka/tables").await;
        let metadata = reqwest::get(format!("{base}/s3/mink/{path}"))
            .await
            .unwrap();
        let (_, cluster) = get(&base, "/admin/cluster").await;
        let metrics = reqwest::get(format!("{base}/metrics")).await.unwrap();
        let text = metrics.text().await.unwrap();

        assert_eq!(config, StatusCode::OK);
        assert_eq!(tables, StatusCode::OK);
        assert_eq!(metadata.status(), StatusCode::OK);
        assert_eq!(metadata.bytes().await.unwrap(), Bytes::from_static(b"{}"));
        assert_eq!(
            cluster["sql"],
            json!({ "duckdbEngine": null, "duckdbExtensions": null })
        );
        for line in [
            r#"mink_catalog_requests_total{route="config",status="2xx"} 1"#,
            r#"mink_catalog_requests_total{route="tables",status="2xx"} 1"#,
            r#"mink_s3_requests_total{method="get",object="metadata",status="2xx"} 1"#,
            r#"mink_admin_requests_total{status="2xx"} 1"#,
        ] {
            assert!(text.contains(line), "{line}\n{text}");
        }
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_node_without_the_catalog_and_s3_roles_serves_no_sql() {
        let Some((node, base)) = admin(&[Role::Admin], |_| {}).await else {
            return;
        };

        let (catalog, _) = get(&base, "/catalog/v1/config").await;
        let (s3, _) = get(&base, "/s3/mink/warehouse/any").await;
        let (_, cluster) = get(&base, "/admin/cluster").await;

        assert_eq!(catalog, StatusCode::NOT_FOUND);
        assert_eq!(s3, StatusCode::NOT_FOUND);
        assert_eq!(cluster["sql"], Value::Null);
        node.shutdown().await.unwrap();
    }
}
