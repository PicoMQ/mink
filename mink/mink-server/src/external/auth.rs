use std::time::{Duration, Instant, SystemTime};

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use reqwest::Request;
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::config::ExternalCatalogAuth;

const RENEW_BEFORE: Duration = Duration::from_secs(5 * 60);

pub(super) enum Authorizer {
    Anonymous,
    Bearer(String),
    Sigv4(Sigv4),
    Oauth2(Oauth2),
}

pub(super) struct Sigv4 {
    service: String,
    region: String,
    provider: Option<SharedCredentialsProvider>,
    credentials: Mutex<Option<Credentials>>,
}

pub(super) struct Oauth2 {
    http: reqwest::Client,
    token_uri: String,
    client_id: Option<String>,
    client_secret: String,
    scope: Option<String>,
    token: Mutex<Option<Token>>,
}

struct Token {
    value: String,
    renew_at: Option<Instant>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: Option<u64>,
}

impl Authorizer {
    pub(super) async fn new(
        auth: Option<&ExternalCatalogAuth>,
        uri: &str,
        http: &reqwest::Client,
    ) -> Authorizer {
        match auth {
            None => Authorizer::Anonymous,
            Some(ExternalCatalogAuth::Bearer { token }) => Authorizer::Bearer(token.clone()),
            Some(ExternalCatalogAuth::Sigv4 { service, region }) => {
                let config = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(region.clone()))
                    .load()
                    .await;

                Authorizer::Sigv4(Sigv4 {
                    service: service.clone(),
                    region: region.clone(),
                    provider: config.credentials_provider(),
                    credentials: Mutex::new(None),
                })
            }
            Some(ExternalCatalogAuth::Oauth2 {
                credential,
                scope,
                token_uri,
            }) => {
                let (client_id, client_secret) = match credential.split_once(':') {
                    Some((id, secret)) => (Some(id.to_owned()), secret.to_owned()),
                    None => (None, credential.clone()),
                };

                Authorizer::Oauth2(Oauth2 {
                    http: http.clone(),
                    token_uri: token_uri
                        .clone()
                        .unwrap_or_else(|| format!("{uri}/v1/oauth/tokens")),
                    client_id,
                    client_secret,
                    scope: scope.clone(),
                    token: Mutex::new(None),
                })
            }
        }
    }

    pub(super) async fn authorize(&self, request: &mut Request) -> Result<(), String> {
        match self {
            Authorizer::Anonymous => Ok(()),
            Authorizer::Bearer(token) => bearer(request, token),
            Authorizer::Oauth2(oauth2) => {
                let token = oauth2.token().await?;

                bearer(request, &token)
            }
            Authorizer::Sigv4(sigv4) => {
                let credentials = sigv4.credentials().await?;

                sign_v4(
                    request,
                    &credentials,
                    &sigv4.region,
                    &sigv4.service,
                    SystemTime::now(),
                )
            }
        }
    }

    pub(super) async fn invalidate(&self) -> bool {
        match self {
            Authorizer::Oauth2(oauth2) => {
                *oauth2.token.lock().await = None;
                true
            }
            Authorizer::Anonymous | Authorizer::Bearer(_) | Authorizer::Sigv4(_) => false,
        }
    }
}

impl Sigv4 {
    async fn credentials(&self) -> Result<Credentials, String> {
        let mut cached = self.credentials.lock().await;

        let renew_at = SystemTime::now() + RENEW_BEFORE;
        if let Some(current) = cached
            .as_ref()
            .filter(|current| current.expiry().is_none_or(|expiry| expiry > renew_at))
        {
            return Ok(current.clone());
        }

        let provider = self
            .provider
            .as_ref()
            .ok_or("no AWS credentials are configured")?;
        let fresh = provider
            .provide_credentials()
            .await
            .map_err(|error| error.to_string())?;

        *cached = Some(fresh.clone());
        Ok(fresh)
    }
}

impl Oauth2 {
    async fn token(&self) -> Result<String, String> {
        let mut cached = self.token.lock().await;

        if let Some(current) = cached
            .as_ref()
            .filter(|current| current.renew_at.is_none_or(|at| at > Instant::now()))
        {
            return Ok(current.value.clone());
        }

        let mut form = vec![
            ("grant_type", "client_credentials"),
            ("client_secret", &self.client_secret),
        ];
        if let Some(client_id) = &self.client_id {
            form.push(("client_id", client_id));
        }
        if let Some(scope) = &self.scope {
            form.push(("scope", scope));
        }

        let response = self
            .http
            .post(&self.token_uri)
            .form(&form)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !response.status().is_success() {
            return Err(format!("the token endpoint answered {}", response.status()));
        }
        let issued: TokenResponse = response.json().await.map_err(|error| error.to_string())?;

        let renew_at = issued.expires_in.map(|seconds| {
            Instant::now() + Duration::from_secs(seconds).saturating_sub(RENEW_BEFORE)
        });
        *cached = Some(Token {
            value: issued.access_token.clone(),
            renew_at,
        });
        Ok(issued.access_token)
    }
}

fn bearer(request: &mut Request, token: &str) -> Result<(), String> {
    let value =
        HeaderValue::from_str(&format!("Bearer {token}")).map_err(|error| error.to_string())?;

    request.headers_mut().insert(AUTHORIZATION, value);
    Ok(())
}

fn sign_v4(
    request: &mut Request,
    credentials: &Credentials,
    region: &str,
    service: &str,
    time: SystemTime,
) -> Result<(), String> {
    let identity = credentials.clone().into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name(service)
        .time(time)
        .settings(SigningSettings::default())
        .build()
        .map_err(|error| error.to_string())?
        .into();

    let headers = request
        .headers()
        .iter()
        .filter_map(|(name, value)| Some((name.as_str(), value.to_str().ok()?)));
    let body = request
        .body()
        .and_then(|body| body.as_bytes())
        .unwrap_or_default();
    let signable = SignableRequest::new(
        request.method().as_str(),
        request.url().as_str(),
        headers,
        SignableBody::Bytes(body),
    )
    .map_err(|error| error.to_string())?;
    let (instructions, _) = sign(signable, &params)
        .map_err(|error| error.to_string())?
        .into_parts();

    let (signed, _) = instructions.into_parts();
    for header in signed {
        let name =
            HeaderName::from_bytes(header.name().as_bytes()).map_err(|error| error.to_string())?;
        let value = HeaderValue::from_str(header.value()).map_err(|error| error.to_string())?;

        request.headers_mut().insert(name, value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use aws_credential_types::Credentials;
    use reqwest::header::AUTHORIZATION;
    use reqwest::{Method, Request};

    use super::sign_v4;

    #[test]
    fn requests_are_signed_as_the_aws_example_says() {
        let credentials = Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None,
            None,
            "example",
        );
        let mut request = Request::new(
            Method::GET,
            "https://example.amazonaws.com/".parse().unwrap(),
        );
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_440_938_160);

        sign_v4(&mut request, &credentials, "us-east-1", "service", time).unwrap();

        assert_eq!(request.headers()["x-amz-date"], "20150830T123600Z");
        assert_eq!(
            request.headers()[AUTHORIZATION],
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }
}
