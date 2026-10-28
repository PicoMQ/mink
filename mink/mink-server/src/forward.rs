use std::time::Duration;

use axum::http::{HeaderValue, Method, header};
use reqwest::{Client, Response};
use uuid::Uuid;

use crate::Error;

pub(crate) const FORWARDED_BY: &str = "mink-forwarded-by";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) enum Wait {
    FirstByte(Duration),
    Whole(Duration),
}

pub(crate) struct Forwarder {
    id: Uuid,
    http: Client,
}

impl Forwarder {
    pub(crate) fn new(id: Uuid, wait: Wait) -> Result<Forwarder, Error> {
        let builder = Client::builder().connect_timeout(CONNECT_TIMEOUT);

        let builder = match wait {
            Wait::FirstByte(limit) => builder.read_timeout(limit),
            Wait::Whole(limit) => builder.timeout(limit),
        };
        let http = builder.build().map_err(Error::HttpClient)?;

        Ok(Forwarder { id, http })
    }

    pub(crate) async fn forward(
        &self,
        address: &str,
        method: Method,
        path: &str,
        range: Option<&HeaderValue>,
    ) -> Result<Response, Error> {
        let url = format!("{}{path}", address.trim_end_matches('/'));

        let mut request = self
            .http
            .request(method, url)
            .header(FORWARDED_BY, self.id.to_string());
        if let Some(range) = range {
            request = request.header(header::RANGE, range);
        }

        request.send().await.map_err(Error::Forward)
    }
}
