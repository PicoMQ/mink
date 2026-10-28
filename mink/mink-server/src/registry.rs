use std::time::Duration;

use mink_metrics::{Answer, Metrics};
use mink_schema::{Reference, RegistrySchema, Rejection, SchemaFormat, SchemaSet};
use mink_store::Store;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::Error;
use crate::config::SchemaRegistry;

const DEFAULT_CONTEXT: &str = ".";
const TIMEOUT: Duration = Duration::from_secs(10);
const LAGGING_REGISTRY: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(2)];

#[derive(Debug)]
pub(crate) enum Failure {
    Rejected(Rejection),
    Failed(Error),
}

impl From<Error> for Failure {
    fn from(error: Error) -> Failure {
        Failure::Failed(error)
    }
}

impl From<mink_store::Error> for Failure {
    fn from(error: mink_store::Error) -> Failure {
        Failure::Failed(error.into())
    }
}

impl From<Rejection> for Failure {
    fn from(rejection: Rejection) -> Failure {
        Failure::Rejected(rejection)
    }
}

pub(crate) struct Registry {
    http: Client,
    settings: SchemaRegistry,
    metrics: Metrics,
}

#[derive(Deserialize)]
struct SchemaResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<ReferenceResponse>,
}

#[derive(Deserialize)]
struct ReferenceResponse {
    name: String,
    subject: String,
    version: i32,
}

#[derive(Deserialize)]
struct SubjectVersion {
    subject: String,
}

#[derive(Deserialize)]
struct VersionResponse {
    id: i32,
}

#[derive(Deserialize)]
struct ContextId {
    context: String,
    id: i32,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error_code: i64,
}

impl Registry {
    pub(crate) fn new(settings: SchemaRegistry, metrics: Metrics) -> Result<Registry, Error> {
        let http = Client::builder()
            .timeout(TIMEOUT)
            .build()
            .map_err(|error| Error::RegistryUnavailable(error.to_string()))?;

        Ok(Registry {
            http,
            settings,
            metrics,
        })
    }

    pub(crate) fn url(&self) -> &str {
        &self.settings.url
    }

    pub(crate) async fn reachable(&self) -> bool {
        let subjects: Result<Option<Vec<String>>, Error> = self.get("/subjects").await;

        subjects.is_ok()
    }

    pub(crate) async fn subject_exists(&self, subject: &str) -> Result<bool, Error> {
        let versions: Option<Vec<i32>> = self
            .get(&format!("/subjects/{subject}/versions?deleted=true"))
            .await?;

        Ok(versions.is_some())
    }

    pub(crate) async fn accepted(&self, id: i32, subject: &str) -> Result<bool, Error> {
        let versions: Option<Vec<SubjectVersion>> = self
            .get(&format!("/schemas/ids/{id}/versions?deleted=true"))
            .await?;

        Ok(versions
            .unwrap_or_default()
            .iter()
            .any(|version| version.subject == subject))
    }

    pub(crate) async fn guid_id(&self, guid: Uuid) -> Result<Option<i32>, Error> {
        let ids: Option<Vec<ContextId>> = self.get(&format!("/schemas/guids/{guid}/ids")).await?;

        let id = ids
            .unwrap_or_default()
            .into_iter()
            .find(|id| id.context == DEFAULT_CONTEXT);
        Ok(id.map(|id| id.id))
    }

    async fn schema(&self, id: i32) -> Result<RegistrySchema, Failure> {
        let Some(response) = self
            .get::<SchemaResponse>(&format!("/schemas/ids/{id}"))
            .await?
        else {
            return Err(Failure::Rejected(Rejection::Missing));
        };
        let Some(format) = format(response.schema_type.as_deref()) else {
            return Err(Failure::Rejected(Rejection::Unsupported));
        };

        let mut references = Vec::with_capacity(response.references.len());
        for reference in &response.references {
            let path = format!(
                "/subjects/{}/versions/{}?deleted=true",
                reference.subject, reference.version
            );

            match self.get::<VersionResponse>(&path).await? {
                Some(version) => references.push(Reference::new(&reference.name, version.id)),
                None => return Err(Failure::Rejected(Rejection::Missing)),
            }
        }

        Ok(RegistrySchema::new(id, format, response.schema, references))
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>, Error> {
        let answer = self.request(path).await;

        let counted = match &answer {
            Ok(Some(_)) => Answer::Ok,
            Ok(None) => Answer::Missing,
            Err(_) => Answer::Error,
        };
        self.metrics.registry_request(counted);
        answer
    }

    async fn request<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>, Error> {
        let url = format!("{}{path}", self.settings.url.trim_end_matches('/'));
        let mut request = self.http.get(&url);

        if let Some(username) = &self.settings.username {
            request = request.basic_auth(username, self.settings.password.as_ref());
        }

        let response = request
            .send()
            .await
            .map_err(|error| Error::RegistryUnavailable(error.to_string()))?;
        let status = response.status();

        if status == StatusCode::NOT_FOUND {
            let error: Option<ErrorResponse> = response.json().await.ok();

            return match error {
                Some(error) if (40400..40500).contains(&error.error_code) => Ok(None),
                _ => Err(Error::RegistryUnavailable(format!("{url} answered 404"))),
            };
        }
        if !status.is_success() {
            return Err(Error::RegistryUnavailable(format!(
                "{url} answered {status}"
            )));
        }

        let body = response.json().await;
        body.map(Some)
            .map_err(|error| Error::RegistryResponse(format!("{url}: {error}")))
    }
}

pub(crate) async fn stored(store: &Store, id: i32) -> Result<SchemaSet, Error> {
    SchemaSet::load(id, |next| async move {
        let stored = store.registry().get(next).await?;

        stored.ok_or(Error::UnstoredRegistrySchema(next))
    })
    .await
}

pub(crate) async fn fetch(
    store: &Store,
    registry: &Registry,
    id: i32,
) -> Result<SchemaSet, Failure> {
    SchemaSet::load(id, |next| async move {
        if let Some(stored) = store.registry().get(next).await? {
            return Ok(stored);
        }

        let fetched = registry.schema(next).await?;

        store.registry().put(&fetched).await?;
        Ok(fetched)
    })
    .await
}

pub(crate) async fn retrying<T, F>(attempt: impl Fn() -> F) -> Result<T, Failure>
where
    F: Future<Output = Result<T, Failure>>,
{
    let mut answer = attempt().await;

    for wait in LAGGING_REGISTRY {
        let lagging = matches!(
            answer,
            Err(Failure::Rejected(
                Rejection::Missing | Rejection::NotAccepted
            ))
        );
        if !lagging {
            break;
        }

        tokio::time::sleep(wait).await;
        answer = attempt().await;
    }

    answer
}

fn format(schema_type: Option<&str>) -> Option<SchemaFormat> {
    let name = schema_type.unwrap_or("AVRO").to_lowercase();

    SchemaFormat::from_name(&name)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::services::Services;
    use crate::testing;

    const UNREACHABLE: &str = "http://127.0.0.1:9";

    struct Setup {
        services: Arc<Services>,
        url: String,
        dependency: i32,
        main: i32,
        subject: String,
    }

    fn client(url: &str) -> Registry {
        let settings = SchemaRegistry {
            url: url.to_owned(),
            username: None,
            password: None,
        };

        Registry::new(settings, Metrics::new()).unwrap()
    }

    async fn setup() -> Option<Setup> {
        let url = mink_conformance::service("TEST_SCHEMA_REGISTRY")?;
        let services = testing::services(|_| {}).await?;
        let namespace = format!("t{}", Uuid::new_v4().simple());
        let subject = format!("{namespace}-value");

        let dependency_schema = json!({"type": "record", "name": "Dep", "namespace": namespace,
            "fields": [{"name": "x", "type": "long"}]});
        let dependency_subject = format!("{namespace}-dep");
        let dependency_body = json!({ "schema": dependency_schema.to_string() });
        let dependency =
            mink_conformance::register_schema(&url, &dependency_subject, &dependency_body).await;

        let main_schema = json!({"type": "record", "name": "Main", "namespace": namespace,
            "fields": [{"name": "d", "type": format!("{namespace}.Dep")}]});
        let reference = json!({"name": format!("{namespace}.Dep"), "subject": dependency_subject, "version": 1});
        let main_body = json!({ "schema": main_schema.to_string(), "references": [reference] });
        let main = mink_conformance::register_schema(&url, &subject, &main_body).await;

        Some(Setup {
            services,
            url,
            dependency,
            main,
            subject,
        })
    }

    #[tokio::test]
    async fn schemas_are_fetched_once_with_their_references() {
        let Some(setup) = setup().await else {
            return;
        };
        let store = &setup.services.store;

        let fetched = fetch(store, &client(&setup.url), setup.main).await.unwrap();
        let again = fetch(store, &client(UNREACHABLE), setup.main)
            .await
            .unwrap();
        let offline = stored(store, setup.main).await.unwrap();

        assert_eq!(fetched.schema().references()[0].id(), setup.dependency);
        assert_eq!(fetched.referenced().len(), 1);
        assert_eq!(again, fetched);
        assert_eq!(offline, fetched);
    }

    #[tokio::test]
    async fn guids_resolve_to_the_ids_they_name() {
        let Some(setup) = setup().await else {
            return;
        };
        let guid = mink_conformance::schema_guid(&setup.url, setup.main).await;
        let client = client(&setup.url);

        let resolved = client.guid_id(guid).await.unwrap();
        let unknown = client.guid_id(Uuid::new_v4()).await.unwrap();

        assert_eq!(resolved, Some(setup.main));
        assert_eq!(unknown, None);
    }

    #[tokio::test]
    async fn only_schemas_under_the_topic_subjects_are_accepted() {
        let Some(setup) = setup().await else {
            return;
        };
        let client = client(&setup.url);

        let under_subject = client.accepted(setup.main, &setup.subject).await.unwrap();
        let elsewhere = client
            .accepted(setup.dependency, &setup.subject)
            .await
            .unwrap();
        let present = client.subject_exists(&setup.subject).await.unwrap();
        let absent = client.subject_exists("absent-value").await.unwrap();

        assert!(under_subject);
        assert!(!elsewhere);
        assert!(present);
        assert!(!absent);
    }

    #[tokio::test]
    async fn missing_schemas_and_an_unreachable_registry_are_told_apart() {
        let Some(setup) = setup().await else {
            return;
        };
        let store = &setup.services.store;
        let unknown = i32::MAX - i32::try_from(Uuid::new_v4().as_u128() % 1_000_000).unwrap();

        let missing = fetch(store, &client(&setup.url), unknown).await;
        let failed = fetch(store, &client(UNREACHABLE), unknown).await;
        let unstored = stored(store, unknown).await;

        assert!(
            matches!(missing, Err(Failure::Rejected(Rejection::Missing))),
            "{missing:?}"
        );
        assert!(
            matches!(failed, Err(Failure::Failed(Error::RegistryUnavailable(_)))),
            "{failed:?}"
        );
        assert!(matches!(unstored, Err(Error::UnstoredRegistrySchema(id)) if id == unknown));
        assert_eq!(store.registry().get(unknown).await.unwrap(), None);
    }

    #[test]
    fn schema_types_mink_does_not_know_are_unsupported() {
        assert_eq!(format(None), Some(SchemaFormat::Avro));
        assert_eq!(format(Some("PROTOBUF")), Some(SchemaFormat::Protobuf));
        assert_eq!(format(Some("JSON")), Some(SchemaFormat::Json));
        assert_eq!(format(Some("XML")), None);
    }
}
