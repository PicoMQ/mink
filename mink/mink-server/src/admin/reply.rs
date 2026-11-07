use std::fmt::Display;
use std::str::FromStr;

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{FromRequest, FromRequestParts, Path, Query, Request};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use iceberg::ErrorKind;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, de};
use serde_json::json;

use crate::Error;
use crate::config::ConfigError;

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
            Error::Store(
                mink_store::Error::NoTable { .. } | mink_store::Error::NoExternalTable { .. },
            )
            | Error::NoSuchTable(_)
            | Error::NoSuchTopic(_)
            | Error::NoSuchKey(_) => StatusCode::NOT_FOUND,
            Error::Store(
                mink_store::Error::AlreadyTable { .. }
                | mink_store::Error::TableRemoving { .. }
                | mink_store::Error::AlreadyExternalTable { .. }
                | mink_store::Error::ExternalTableTaken { .. }
                | mink_store::Error::OverlappingLocation { .. },
            )
            | Error::NotIndexing(_)
            | Error::ConflictingEndOffsets(_) => StatusCode::CONFLICT,
            Error::Store(
                mink_store::Error::NotABoundary { .. }
                | mink_store::Error::RangeNotStarted(_)
                | mink_store::Error::EndOffsetBeyondIndex { .. },
            )
            | Error::Core(_)
            | Error::Iceberg(
                mink_iceberg::Error::ExternalSchema
                | mink_iceberg::Error::ExternalPartitionField(_)
                | mink_iceberg::Error::EqualityDeletes
                | mink_iceberg::Error::FormatVersion(_)
                | mink_iceberg::Error::TableProperty { .. },
            )
            | Error::Config(ConfigError::Duration(_))
            | Error::NoSuchExternalCatalog(_)
            | Error::UnreadableTerm(_)
            | Error::UnknownColumn(_)
            | Error::ShortRetention { .. }
            | Error::LocationInWarehouse(_)
            | Error::MissingEndOffsets(_)
            | Error::InvalidEndOffset { .. }
            | Error::ExistingTableKeeps(_)
            | Error::EndOffsetsOfNewTable
            | Error::PartitioningRequired => StatusCode::BAD_REQUEST,
            Error::Iceberg(mink_iceberg::Error::Iceberg(error)) => catalog_status(error),
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

fn catalog_status(error: &iceberg::Error) -> StatusCode {
    match error.kind() {
        ErrorKind::TableAlreadyExists
        | ErrorKind::NamespaceAlreadyExists
        | ErrorKind::CatalogCommitConflicts => StatusCode::CONFLICT,
        ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound => StatusCode::NOT_FOUND,
        ErrorKind::DataInvalid => StatusCode::BAD_REQUEST,
        _ => StatusCode::BAD_GATEWAY,
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
