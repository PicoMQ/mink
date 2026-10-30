use std::fmt::Display;
use std::str::FromStr;

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{FromRequest, FromRequestParts, Path, Query, Request};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, de};
use serde_json::json;

use crate::Error;

#[derive(Debug)]
pub(super) struct Failure {
    status: StatusCode,
    message: String,
}

impl Failure {
    pub(super) fn bad(message: impl Into<String>) -> Failure {
        Failure {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub(super) fn missing(message: impl Into<String>) -> Failure {
        Failure {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub(super) fn not_allowed(message: impl Into<String>) -> Failure {
        Failure {
            status: StatusCode::METHOD_NOT_ALLOWED,
            message: message.into(),
        }
    }

    pub(super) fn unreachable(message: impl Into<String>) -> Failure {
        Failure {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
        }
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Failure {
        let status = match &error {
            _ if error.store_unavailable() => StatusCode::SERVICE_UNAVAILABLE,
            Error::Store(mink_store::Error::NoTable { .. })
            | Error::NoSuchTable(_)
            | Error::NoSuchTopic(_)
            | Error::NoSuchKey(_) => StatusCode::NOT_FOUND,
            Error::Store(
                mink_store::Error::AlreadyTable { .. } | mink_store::Error::TableRemoving { .. },
            )
            | Error::NotIndexing(_) => StatusCode::CONFLICT,
            Error::Kafka(_)
            | Error::TopicUnavailable(_)
            | Error::RegistryUnavailable(_)
            | Error::RegistryResponse(_)
            | Error::Forward(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        Failure {
            status,
            message: error.to_string(),
        }
    }
}

impl From<mink_store::Error> for Failure {
    fn from(error: mink_store::Error) -> Failure {
        Failure::from(Error::from(error))
    }
}

impl From<mink_kafka::Error> for Failure {
    fn from(error: mink_kafka::Error) -> Failure {
        Failure::from(Error::from(error))
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

pub(super) struct Params<T>(pub(super) T);

pub(super) struct Segments<T>(pub(super) T);

pub(super) struct Body<T>(pub(super) T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequestParts<S> for Params<T> {
    type Rejection = Failure;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Failure> {
        let Query(value) = Query::<T>::from_request_parts(parts, state)
            .await
            .map_err(|rejection: QueryRejection| Failure::bad(rejection.body_text()))?;

        Ok(Params(value))
    }
}

impl<S: Send + Sync, T: DeserializeOwned + Send> FromRequestParts<S> for Segments<T> {
    type Rejection = Failure;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Failure> {
        let Path(value) = Path::<T>::from_request_parts(parts, state)
            .await
            .map_err(|rejection: PathRejection| Failure::bad(rejection.body_text()))?;

        Ok(Segments(value))
    }
}

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for Body<T> {
    type Rejection = Failure;

    async fn from_request(request: Request, state: &S) -> Result<Self, Failure> {
        let Json(value) = Json::<T>::from_request(request, state)
            .await
            .map_err(|rejection: JsonRejection| Failure::bad(rejection.body_text()))?;

        Ok(Body(value))
    }
}

pub(super) fn filter<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: Display,
{
    let text = Option::<String>::deserialize(deserializer)?;

    match text.filter(|text| !text.is_empty()) {
        Some(text) => text.parse().map(Some).map_err(de::Error::custom),
        None => Ok(None),
    }
}
