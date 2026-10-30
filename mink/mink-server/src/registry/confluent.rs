use std::time::Duration;

use async_trait::async_trait;
use mink_core::Part;
use mink_metrics::{Answer, Metrics};
use mink_schema::{
    Reference, RegistrySchema, Rejection, SchemaFormat, SchemaId, SchemaKey, subject,
};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::{Failure, Registry};
use crate::Error;

const DEFAULT_CONTEXT: &str = ".";
const TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct Confluent {
    http: Client,
    url: String,
    username: Option<String>,
    password: Option<String>,
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

impl Confluent {
    pub(crate) fn new(
        url: String,
        username: Option<String>,
        password: Option<String>,
        metrics: Metrics,
    ) -> Result<Confluent, Error> {
        let http = Client::builder()
            .timeout(TIMEOUT)
            .build()
            .map_err(|error| Error::RegistryUnavailable(error.to_string()))?;

        Ok(Confluent {
            http,
            url,
            username,
            password,
            metrics,
        })
    }

    async fn guid_id(&self, guid: Uuid) -> Result<Option<i32>, Error> {
        let ids: Option<Vec<ContextId>> = self.get(&format!("/schemas/guids/{guid}/ids")).await?;

        let id = ids
            .unwrap_or_default()
            .into_iter()
            .find(|id| id.context == DEFAULT_CONTEXT);
        Ok(id.map(|id| id.id))
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
        let url = format!("{}{path}", self.url.trim_end_matches('/'));
        let mut request = self.http.get(&url);

        if let Some(username) = &self.username {
            request = request.basic_auth(username, self.password.as_ref());
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

#[async_trait]
impl Registry for Confluent {
    async fn reachable(&self) -> bool {
        let subjects: Result<Option<Vec<String>>, Error> = self.get("/subjects").await;

        subjects.is_ok()
    }

    async fn resolve(&self, schema_id: SchemaId) -> Result<SchemaKey, Failure> {
        match schema_id {
            SchemaId::Key(SchemaKey::Confluent(id)) => Ok(SchemaKey::Confluent(id)),
            SchemaId::ConfluentGuid(guid) => match self.guid_id(guid).await? {
                Some(id) => Ok(SchemaKey::Confluent(id)),
                None => Err(Failure::Rejected(Rejection::Missing)),
            },
            SchemaId::Key(SchemaKey::Glue(_)) => Err(Failure::Rejected(Rejection::OtherRegistry)),
        }
    }

    async fn schema(&self, key: SchemaKey) -> Result<RegistrySchema, Failure> {
        let SchemaKey::Confluent(id) = key else {
            return Err(Failure::Rejected(Rejection::OtherRegistry));
        };

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
                Some(version) => references.push(Reference::new(
                    &reference.name,
                    SchemaKey::Confluent(version.id),
                )),
                None => return Err(Failure::Rejected(Rejection::Missing)),
            }
        }

        Ok(RegistrySchema::new(
            key,
            format,
            response.schema,
            references,
        ))
    }

    async fn accepted(&self, topic: &str, part: Part, key: SchemaKey) -> Result<bool, Failure> {
        let SchemaKey::Confluent(id) = key else {
            return Err(Failure::Rejected(Rejection::OtherRegistry));
        };
        let subject = subject(topic, part);

        let Some(versions) = self
            .get::<Vec<SubjectVersion>>(&format!("/schemas/ids/{id}/versions?deleted=true"))
            .await?
        else {
            return Err(Failure::Rejected(Rejection::Missing));
        };

        Ok(versions.iter().any(|version| version.subject == subject))
    }
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
    use crate::registry::{fetch, stored};
    use crate::services::Services;
    use crate::testing;

    const UNREACHABLE: &str = "http://127.0.0.1:9";

    struct Setup {
        services: Arc<Services>,
        url: String,
        dependency: i32,
        main: i32,
        topic: String,
    }

    fn client(url: &str) -> Confluent {
        Confluent::new(url.to_owned(), None, None, Metrics::new()).unwrap()
    }

    async fn setup() -> Option<Setup> {
        let url = mink_conformance::service("TEST_SCHEMA_REGISTRY")?;
        let services = testing::services(|_| {}).await?;
        let topic = format!("t{}", Uuid::new_v4().simple());
        let namespace = topic.clone();
        let subject = format!("{topic}-value");

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
            topic,
        })
    }

    #[tokio::test]
    async fn schemas_are_fetched_once_with_their_references() {
        let Some(setup) = setup().await else {
            return;
        };
        let store = &setup.services.store;

        let main = SchemaKey::Confluent(setup.main);
        let fetched = fetch(store, &client(&setup.url), main).await.unwrap();
        let again = fetch(store, &client(UNREACHABLE), main).await.unwrap();
        let offline = stored(store, main).await.unwrap();

        assert_eq!(
            fetched.schema().references()[0].key(),
            SchemaKey::Confluent(setup.dependency)
        );
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

        let resolved = client.resolve(SchemaId::ConfluentGuid(guid)).await;
        let unknown = client
            .resolve(SchemaId::ConfluentGuid(Uuid::new_v4()))
            .await;

        assert!(
            matches!(resolved, Ok(key) if key == SchemaKey::Confluent(setup.main)),
            "{resolved:?}"
        );
        assert!(
            matches!(unknown, Err(Failure::Rejected(Rejection::Missing))),
            "{unknown:?}"
        );
    }

    #[tokio::test]
    async fn only_schemas_under_the_topic_subjects_are_accepted() {
        let Some(setup) = setup().await else {
            return;
        };
        let client = client(&setup.url);

        let main = SchemaKey::Confluent(setup.main);
        let dependency = SchemaKey::Confluent(setup.dependency);

        let under_subject = client.accepted(&setup.topic, Part::Value, main).await;
        let elsewhere = client.accepted(&setup.topic, Part::Value, dependency).await;
        let other_part = client.accepted(&setup.topic, Part::Key, main).await;
        let absent = client.accepted("absent", Part::Value, main).await;

        assert!(matches!(under_subject, Ok(true)), "{under_subject:?}");
        assert!(matches!(elsewhere, Ok(false)), "{elsewhere:?}");
        assert!(matches!(other_part, Ok(false)), "{other_part:?}");
        assert!(matches!(absent, Ok(false)), "{absent:?}");
    }

    #[tokio::test]
    async fn missing_schemas_and_an_unreachable_registry_are_told_apart() {
        let Some(setup) = setup().await else {
            return;
        };
        let store = &setup.services.store;
        let unknown = SchemaKey::Confluent(
            i32::MAX - i32::try_from(Uuid::new_v4().as_u128() % 1_000_000).unwrap(),
        );

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
        assert!(matches!(unstored, Err(Error::UnstoredRegistrySchema(key)) if key == unknown));
        assert_eq!(store.registry().get(unknown).await.unwrap(), None);
    }

    #[tokio::test]
    async fn ids_resolve_to_themselves_and_glue_versions_to_another_registry() {
        let client = client(UNREACHABLE);

        let own = client.resolve(SchemaId::Key(SchemaKey::Confluent(7))).await;
        let glue = client
            .resolve(SchemaId::Key(SchemaKey::Glue(Uuid::new_v4())))
            .await;

        assert!(matches!(own, Ok(SchemaKey::Confluent(7))), "{own:?}");
        assert!(
            matches!(glue, Err(Failure::Rejected(Rejection::OtherRegistry))),
            "{glue:?}"
        );
    }

    #[test]
    fn schema_types_mink_does_not_know_are_unsupported() {
        assert_eq!(format(None), Some(SchemaFormat::Avro));
        assert_eq!(format(Some("PROTOBUF")), Some(SchemaFormat::Protobuf));
        assert_eq!(format(Some("JSON")), Some(SchemaFormat::Json));
        assert_eq!(format(Some("XML")), None);
    }
}
