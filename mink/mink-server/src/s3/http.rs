use std::io;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, HttpBody};
use axum::extract::{Path, State};
use axum::http::{self, HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use futures::TryStreamExt;
use mink_metrics::{Method, Object};
use nestor::{ReadRange, ReadStream};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::cache::Cache;
use super::key::Key;
use super::reader::{Found, Reader, Request};
use crate::Error;
use crate::forward::FORWARDED_BY;
use crate::services::Services;

const SERVED_BY: &str = "mink-served-by";
const RELAYED_HEADERS: [&str; 5] = [
    "content-length",
    "content-range",
    "content-type",
    "etag",
    SERVED_BY,
];

pub(crate) fn router(services: Arc<Services>, cache: Arc<Cache>) -> Result<Router, Error> {
    let reader = Reader::new(services, cache)?;

    Ok(Router::new()
        .route("/{bucket}", get(bucket).put(forbidden).delete(forbidden))
        .route(
            "/{bucket}/{*key}",
            get(read)
                .head(head)
                .put(forbidden)
                .post(forbidden)
                .delete(forbidden),
        )
        .with_state(Arc::new(reader)))
}

async fn read(
    State(reader): State<Arc<Reader>>,
    Path((bucket, name)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let key = match reader.key(&format!("s3://{bucket}/{name}")) {
        Ok(key) => key,
        Err(error) => return counted(&reader, Method::Get, None, failure(&error), false),
    };
    let request = Request {
        path: uri.path(),
        range: headers.get(header::RANGE),
        forwarded: headers.contains_key(FORWARDED_BY),
    };

    let range = request
        .range
        .and_then(|range| range.to_str().ok())
        .and_then(read_range);

    let found = reader
        .get(&key, &request, range.clone().unwrap_or(ReadRange::Full))
        .await;
    let relayed = matches!(found, Ok(Found::Forwarded(_)));

    let response = answer(&reader, &name, range.as_ref(), found);
    counted(&reader, Method::Get, Some(&key), response, relayed)
}

async fn head(
    State(reader): State<Arc<Reader>>,
    Path((bucket, name)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let key = match reader.key(&format!("s3://{bucket}/{name}")) {
        Ok(key) => key,
        Err(error) => return counted(&reader, Method::Head, None, failure(&error), false),
    };
    let request = Request {
        path: uri.path(),
        range: None,
        forwarded: headers.contains_key(FORWARDED_BY),
    };

    let found = reader.head(&key, &request).await;
    let relayed = matches!(found, Ok(Found::Forwarded(_)));

    let response = answer(&reader, &name, None, found);
    counted(&reader, Method::Head, Some(&key), response, relayed)
}

fn answer(
    reader: &Reader,
    name: &str,
    range: Option<&ReadRange>,
    found: Result<Found, Error>,
) -> Response {
    match found {
        Ok(Found::Bytes(bytes)) => served(reader, body(name, bytes, range)),
        Ok(Found::Stream(stream)) => served(reader, streamed(name, stream, range.is_some())),
        Ok(Found::Length(size)) => served(reader, length(name, size)),
        Ok(Found::Forwarded(answer)) => relayed(answer),
        Err(error) => failure(&error),
    }
}

fn counted(
    reader: &Reader,
    method: Method,
    key: Option<&Key>,
    response: Response,
    relayed: bool,
) -> Response {
    let metrics = reader.metrics();
    let object = match key.and_then(Key::file) {
        Some(_) => Object::Data,
        None => Object::Metadata,
    };

    metrics.s3_request(method, object, response.status().as_u16());

    if method == Method::Get && !relayed {
        let sent = response.body().size_hint().exact().or_else(|| {
            response
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|length| length.to_str().ok()?.parse().ok())
        });
        metrics.sent(sent.unwrap_or(0));
    }
    response
}

fn relayed(answer: reqwest::Response) -> Response {
    let status = answer.status();
    let mut headers = HeaderMap::new();
    for name in RELAYED_HEADERS {
        if let Some(value) = answer.headers().get(name) {
            headers.insert(name, value.clone());
        }
    }

    (status, headers, Body::from_stream(answer.bytes_stream())).into_response()
}

fn served(reader: &Reader, mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(SERVED_BY, served_by(reader.id()));

    response
}

fn served_by(id: Uuid) -> HeaderValue {
    HeaderValue::from_str(&id.to_string()).expect("a uuid is a header value")
}

fn length(name: &str, length: u64) -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_LENGTH, length.to_string()),
            (header::ETAG, etag(name)),
        ],
    )
        .into_response()
}

fn body(key: &str, bytes: Bytes, range: Option<&ReadRange>) -> Response {
    let size = bytes.len() as u64;
    let Some(range) = range else {
        return (StatusCode::OK, [(header::ETAG, etag(key))], bytes).into_response();
    };

    match range.resolve(size) {
        Ok(range) => (
            StatusCode::PARTIAL_CONTENT,
            [
                (
                    header::CONTENT_RANGE,
                    format!("bytes {}-{}/{size}", range.start, range.end - 1),
                ),
                (header::ETAG, etag(key)),
            ],
            bytes.slice(range.start as usize..range.end as usize),
        )
            .into_response(),
        Err(_) => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(header::CONTENT_RANGE, format!("bytes */{size}"))],
        )
            .into_response(),
    }
}

fn streamed(key: &str, stream: ReadStream, partial: bool) -> Response {
    let range = stream.range();
    let size = stream.size().unwrap_or(range.end);

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from(range.end - range.start),
    );
    headers.insert(header::ETAG, header_value(&etag(key)));

    let status = match partial {
        true => {
            let content_range = format!("bytes {}-{}/{size}", range.start, range.end - 1);
            headers.insert(header::CONTENT_RANGE, header_value(&content_range));
            StatusCode::PARTIAL_CONTENT
        }
        false => StatusCode::OK,
    };

    let body = Body::from_stream(stream.map_err(io::Error::other));
    (status, headers, body).into_response()
}

fn read_range(range: &str) -> Option<ReadRange> {
    let (first, last) = range.strip_prefix("bytes=")?.split_once('-')?;

    match (first, last) {
        ("", suffix) => Some(ReadRange::Suffix(suffix.parse().ok()?)),
        (first, "") => Some(ReadRange::From(first.parse().ok()?)),
        (first, last) => {
            let start: u64 = first.parse().ok()?;
            let end: u64 = last.parse().ok()?;
            (start <= end).then(|| ReadRange::Bounded(start..end + 1))
        }
    }
}

fn header_value(text: &str) -> HeaderValue {
    HeaderValue::from_str(text).expect("a range or an etag is a header value")
}

fn etag(key: &str) -> String {
    let digest = Sha256::digest(key.as_bytes());

    format!("\"{}\"", hex::encode(&digest[..16]))
}

fn failure(error: &Error) -> Response {
    match error {
        Error::NoSuchKey(path) => s3_error(StatusCode::NOT_FOUND, "NoSuchKey", path),
        Error::Unsatisfiable(size) => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(header::CONTENT_RANGE, format!("bytes */{size}"))],
        )
            .into_response(),
        error if error.store_unavailable() => s3_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            &error.to_string(),
        ),
        error => s3_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &error.to_string(),
        ),
    }
}

async fn bucket(State(reader): State<Arc<Reader>>, method: http::Method) -> Response {
    let response = s3_error(
        StatusCode::NOT_IMPLEMENTED,
        "NotImplemented",
        "not implemented",
    );

    let method = match method {
        http::Method::HEAD => Method::Head,
        _ => Method::Get,
    };
    counted(&reader, method, None, response, false)
}

async fn forbidden() -> Response {
    s3_error(StatusCode::FORBIDDEN, "AccessDenied", "mink is read-only")
}

fn s3_error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code><Message>{message}</Message></Error>"
    );

    (status, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_follow_the_http_forms() {
        let resolved = |text: &str| read_range(text).and_then(|range| range.resolve(10).ok());

        assert_eq!(resolved("bytes=0-3"), Some(0..4));
        assert_eq!(resolved("bytes=4-"), Some(4..10));
        assert_eq!(resolved("bytes=-8"), Some(2..10));
        assert_eq!(resolved("bytes=5-100"), Some(5..10));
        assert_eq!(resolved("bytes=10-12"), None);
        assert_eq!(read_range("items=0-3"), None);
        assert_eq!(read_range("bytes=5-3"), None);
        assert_eq!(read_range("bytes=0-1,4-5"), None);
    }
}
