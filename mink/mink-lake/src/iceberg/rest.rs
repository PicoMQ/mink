//! Commit target over the Iceberg REST catalog, with bearer or OAuth2 client-credential authentication.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent, TableRequirement, TableUpdate};
use mink_common::url::encode;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::iceberg::commit::CommitTarget;

pub(crate) struct RestCommitTarget {
    catalog: Arc<dyn Catalog>,
    http: Client,
    uri: String,
    prefix: Option<String>,
    auth: Auth,
    headers: Vec<(String, String)>,
    token: Mutex<Option<String>>,
}

enum Auth {
    None,
    Token(String),
    ClientCredentials {
        endpoint: String,
        client_id: Option<String>,
        client_secret: String,
        scope: String,
    },
}

#[derive(Serialize)]
struct CommitRequest<'a> {
    identifier: &'a TableIdent,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

#[derive(Deserialize)]
struct CatalogConfig {
    #[serde(default)]
    defaults: HashMap<String, String>,
    #[serde(default)]
    overrides: HashMap<String, String>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

impl RestCommitTarget {
    pub(crate) async fn connect(
        catalog: Arc<dyn Catalog>,
        uri: &str,
        warehouse: Option<&str>,
        props: &HashMap<String, String>,
    ) -> Result<Self> {
        let http = Client::new();
        let uri = uri.trim_end_matches('/').to_string();
        let auth = match (props.get("token"), props.get("credential")) {
            (Some(token), _) => Auth::Token(token.clone()),
            (None, Some(credential)) => {
                let (client_id, client_secret) = match credential.split_once(':') {
                    Some((id, secret)) => (Some(id.to_string()), secret.to_string()),
                    None => (None, credential.clone()),
                };
                Auth::ClientCredentials {
                    endpoint: props
                        .get("oauth2-server-uri")
                        .cloned()
                        .unwrap_or_else(|| format!("{uri}/v1/oauth/tokens")),
                    client_id,
                    client_secret,
                    scope: props
                        .get("scope")
                        .cloned()
                        .unwrap_or_else(|| "catalog".to_string()),
                }
            }
            (None, None) => Auth::None,
        };

        let headers = props
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("header.")
                    .map(|name| (name.to_string(), v.clone()))
            })
            .collect();
        let mut target = RestCommitTarget {
            catalog,
            http,
            uri,
            prefix: props.get("prefix").cloned(),
            auth,
            headers,
            token: Mutex::new(None),
        };

        let mut request = target.http.get(format!("{}/v1/config", target.uri));
        if let Some(warehouse) = warehouse {
            request = request.query(&[("warehouse", warehouse)]);
        }
        let response = target.authorize(request).await?.send().await?;
        if response.status().is_success() {
            let config: CatalogConfig = response.json().await?;
            if let Some(prefix) = config
                .overrides
                .get("prefix")
                .or_else(|| config.defaults.get("prefix"))
            {
                target.prefix = Some(prefix.clone());
            }
        }

        Ok(target)
    }

    fn table_endpoint(&self, ident: &TableIdent) -> String {
        let namespace = ident
            .namespace()
            .as_ref()
            .iter()
            .map(|part| encode(part))
            .collect::<Vec<_>>()
            .join("%1F");
        let prefix = self
            .prefix
            .as_ref()
            .map(|p| format!("{p}/"))
            .unwrap_or_default();

        format!(
            "{}/v1/{prefix}namespaces/{namespace}/tables/{}",
            self.uri,
            encode(ident.name())
        )
    }

    async fn authorize(&self, mut request: RequestBuilder) -> Result<RequestBuilder> {
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        match &self.auth {
            Auth::None => {}
            Auth::Token(token) => request = request.bearer_auth(token),
            Auth::ClientCredentials { .. } => {
                let token = self.token().await?;
                request = request.bearer_auth(token);
            }
        }

        Ok(request)
    }

    async fn token(&self) -> Result<String> {
        let mut cached = self.token.lock().await;
        if let Some(token) = cached.as_ref() {
            return Ok(token.clone());
        }
        let Auth::ClientCredentials {
            endpoint,
            client_id,
            client_secret,
            scope,
        } = &self.auth
        else {
            unreachable!("token() is only called for client credentials");
        };
        let mut form = vec![
            ("grant_type", "client_credentials".to_string()),
            ("client_secret", client_secret.clone()),
            ("scope", scope.clone()),
        ];
        if let Some(id) = client_id {
            form.push(("client_id", id.clone()));
        }

        let response = self.http.post(endpoint).form(&form).send().await?;
        if !response.status().is_success() {
            return Err(Error::Other(format!(
                "oauth2 token exchange failed with {}",
                response.status()
            )));
        }
        let token: TokenResponse = response.json().await?;
        *cached = Some(token.access_token.clone());

        Ok(token.access_token)
    }
}

#[async_trait]
impl CommitTarget for RestCommitTarget {
    async fn load(&self, ident: &TableIdent) -> Result<Table> {
        Ok(self.catalog.load_table(ident).await?)
    }

    async fn commit(
        &self,
        ident: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<Table> {
        let body = CommitRequest {
            identifier: ident,
            requirements,
            updates,
        };
        let post = || async {
            self.authorize(self.http.post(self.table_endpoint(ident)))
                .await?
                .json(&body)
                .send()
                .await
                .map_err(Error::from)
        };
        let mut response = post().await?;
        if response.status() == StatusCode::UNAUTHORIZED
            && matches!(self.auth, Auth::ClientCredentials { .. })
        {
            *self.token.lock().await = None;
            response = post().await?;
        }

        let status = response.status();
        match status {
            StatusCode::OK => Ok(self.catalog.load_table(ident).await?),
            StatusCode::CONFLICT => Err(Error::CommitConflict(
                response.text().await.unwrap_or_default(),
            )),
            StatusCode::NOT_FOUND => Err(Error::Other(format!(
                "iceberg table {ident} does not exist in the REST catalog"
            ))),
            _ => Err(Error::Other(format!(
                "iceberg REST commit failed with {status}: {}",
                response.text().await.unwrap_or_default()
            ))),
        }
    }
}
