use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{MatchedPath, Path, Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use iceberg_catalog_rest::ErrorModel;
use mink_metrics::{Metrics, Route};
use serde::Serialize;
use serde_json::{Value, json};

use super::tables::Tables;
use crate::Error;
use crate::services::Services;

const ENDPOINTS: [&str; 7] = [
    "GET /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "GET /v1/{prefix}/namespaces/{namespace}/views",
];

struct Catalog {
    services: Arc<Services>,
    tables: Tables,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorModel,
}

pub(crate) fn router(services: Arc<Services>) -> Router {
    Router::new()
        .route("/v1/config", get(config))
        .route("/v1/namespaces", get(namespaces).post(forbidden))
        .route(
            "/v1/namespaces/{namespace}",
            get(namespace).head(namespace_exists).delete(forbidden),
        )
        .route("/v1/namespaces/{namespace}/properties", post(forbidden))
        .route(
            "/v1/namespaces/{namespace}/tables",
            get(tables).post(forbidden),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}",
            get(load)
                .head(table_exists)
                .post(forbidden)
                .delete(forbidden),
        )
        .route("/v1/namespaces/{namespace}/register", post(forbidden))
        .route("/v1/tables/rename", post(forbidden))
        .route("/v1/transactions/commit", post(forbidden))
        .route(
            "/v1/namespaces/{namespace}/views",
            get(views).post(forbidden),
        )
        .route(
            "/v1/namespaces/{namespace}/views/{view}",
            get(no_such_view)
                .head(no_such_view)
                .post(forbidden)
                .delete(forbidden),
        )
        .route("/v1/namespaces/{namespace}/register-view", post(forbidden))
        .route("/v1/views/rename", post(forbidden))
        .layer(middleware::from_fn_with_state(
            services.metrics.clone(),
            counted,
        ))
        .with_state(Arc::new(Catalog {
            tables: Tables::new(services.clone()),
            services,
        }))
}

async fn counted(State(metrics): State<Metrics>, request: Request, next: Next) -> Response {
    let matched = request.extensions().get::<MatchedPath>();
    let route = route(request.method(), matched.map_or("", MatchedPath::as_str));

    let response = next.run(request).await;

    metrics.catalog_request(route, response.status().as_u16());
    response
}

fn route(method: &Method, path: &str) -> Route {
    let read = matches!(*method, Method::GET | Method::HEAD);

    match path {
        _ if !read => Route::Write,
        path if path.ends_with("/v1/config") => Route::Config,
        path if path.ends_with("/tables") => Route::Tables,
        path if path.ends_with("/tables/{table}") => Route::Load,
        _ => Route::Namespaces,
    }
}

async fn config(State(catalog): State<Arc<Catalog>>) -> Response {
    Json(json!({
        "defaults": s3_config(&catalog.services),
        "overrides": {},
        "endpoints": ENDPOINTS,
    }))
    .into_response()
}

async fn namespaces(State(catalog): State<Arc<Catalog>>) -> Response {
    Json(json!({ "namespaces": [[catalog.services.config.namespace]] })).into_response()
}

async fn namespace(State(catalog): State<Arc<Catalog>>, Path(namespace): Path<String>) -> Response {
    match require_namespace(&catalog, &namespace) {
        Ok(()) => Json(json!({ "namespace": [namespace], "properties": {} })).into_response(),
        Err(error) => failure(&error),
    }
}

async fn namespace_exists(
    State(catalog): State<Arc<Catalog>>,
    Path(namespace): Path<String>,
) -> Response {
    match require_namespace(&catalog, &namespace) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => failure(&error),
    }
}

async fn tables(State(catalog): State<Arc<Catalog>>, Path(namespace): Path<String>) -> Response {
    let listed = async {
        require_namespace(&catalog, &namespace)?;

        catalog.tables.names().await
    };

    match listed.await {
        Ok(names) => {
            let identifiers: Vec<Value> = names
                .into_iter()
                .map(|name| json!({ "namespace": [namespace], "name": name }))
                .collect();
            Json(json!({ "identifiers": identifiers })).into_response()
        }
        Err(error) => failure(&error),
    }
}

async fn load(
    State(catalog): State<Arc<Catalog>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Response {
    let loaded = async {
        require_namespace(&catalog, &namespace)?;

        catalog.tables.load(&table).await
    };

    match loaded.await {
        Ok(loaded) => match serde_json::from_slice::<Value>(loaded.bytes()) {
            Ok(metadata) => Json(json!({
                "metadata-location": loaded.location(),
                "metadata": metadata,
                "config": s3_config(&catalog.services),
            }))
            .into_response(),
            Err(error) => failure(&Error::Json(error)),
        },
        Err(error) => failure(&error),
    }
}

async fn table_exists(
    State(catalog): State<Arc<Catalog>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Response {
    let exists = async {
        require_namespace(&catalog, &namespace)?;

        match catalog.tables.exists(&table).await? {
            true => Ok(()),
            false => Err(Error::NoSuchTable(table.clone())),
        }
    };

    match exists.await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => failure(&error),
    }
}

async fn views(State(catalog): State<Arc<Catalog>>, Path(namespace): Path<String>) -> Response {
    match require_namespace(&catalog, &namespace) {
        Ok(()) => Json(json!({ "identifiers": [] })).into_response(),
        Err(error) => failure(&error),
    }
}

async fn no_such_view(Path((_, view)): Path<(String, String)>) -> Response {
    error(
        StatusCode::NOT_FOUND,
        "NoSuchViewException",
        &format!("no view {view}"),
    )
}

async fn forbidden() -> Response {
    error(
        StatusCode::FORBIDDEN,
        "ForbiddenException",
        "mink is a read-only catalog",
    )
}

fn require_namespace(catalog: &Catalog, namespace: &str) -> Result<(), Error> {
    match namespace == catalog.services.config.namespace {
        true => Ok(()),
        false => Err(Error::NoSuchNamespace(namespace.to_owned())),
    }
}

fn s3_config(services: &Services) -> Value {
    json!({
        "s3.endpoint": services.config.s3_endpoint,
        "s3.path-style-access": "true",
        "client.region": services.config.storage.region,
    })
}

fn failure(failure: &Error) -> Response {
    match failure {
        Error::NoSuchNamespace(namespace) => error(
            StatusCode::NOT_FOUND,
            "NoSuchNamespaceException",
            &format!("no namespace {namespace}"),
        ),
        Error::NoSuchTable(table) => error(
            StatusCode::NOT_FOUND,
            "NoSuchTableException",
            &format!("no table {table}"),
        ),
        other if other.store_unavailable() => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            &other.to_string(),
        ),
        other => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ServiceFailureException",
            &other.to_string(),
        ),
    }
}

fn error(status: StatusCode, kind: &str, message: &str) -> Response {
    let error = ErrorModel {
        message: message.to_owned(),
        r#type: kind.to_owned(),
        code: status.as_u16(),
        stack: None,
    };

    (status, Json(ErrorResponse { error })).into_response()
}
