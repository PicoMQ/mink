use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_glue::Client;
use aws_sdk_glue::error::SdkError;
use aws_sdk_glue::operation::get_schema_version::{GetSchemaVersionError, GetSchemaVersionOutput};
use aws_sdk_glue::types::{DataFormat, RegistryId, SchemaVersionStatus};
use mink_core::Part;
use mink_metrics::{Answer, Metrics};
use mink_schema::{RegistrySchema, Rejection, SchemaFormat, SchemaId, SchemaKey};
use uuid::Uuid;

use super::{Failure, Registry};
use crate::Error;

pub(crate) struct Glue {
    client: Client,
    name: String,
    metrics: Metrics,
}

impl Glue {
    pub(crate) async fn new(name: String, region: Option<String>, metrics: Metrics) -> Glue {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(Region::new(region));
        }

        let config = loader.load().await;

        Glue {
            client: Client::new(&config),
            name,
            metrics,
        }
    }

    async fn version(&self, version: Uuid) -> Result<GetSchemaVersionOutput, Failure> {
        let answer = self
            .client
            .get_schema_version()
            .schema_version_id(version.to_string())
            .send()
            .await;

        let counted = match &answer {
            Ok(_) => Answer::Ok,
            Err(error) if missing(error) => Answer::Missing,
            Err(_) => Answer::Error,
        };
        self.metrics.registry_request(counted);

        match answer {
            Ok(output) if output.status() == Some(&SchemaVersionStatus::Available) => Ok(output),
            Ok(_) => Err(Failure::Rejected(Rejection::Missing)),
            Err(error) if missing(&error) => Err(Failure::Rejected(Rejection::Missing)),
            Err(error) => Err(Failure::Failed(Error::RegistryUnavailable(
                error.to_string(),
            ))),
        }
    }
}

#[async_trait]
impl Registry for Glue {
    async fn reachable(&self) -> bool {
        let registry = RegistryId::builder().registry_name(&self.name).build();

        let answer = self
            .client
            .get_registry()
            .registry_id(registry)
            .send()
            .await;

        let counted = match &answer {
            Ok(_) => Answer::Ok,
            Err(_) => Answer::Error,
        };
        self.metrics.registry_request(counted);
        answer.is_ok()
    }

    async fn resolve(&self, schema_id: SchemaId) -> Result<SchemaKey, Failure> {
        match schema_id {
            SchemaId::Key(SchemaKey::Glue(version)) => Ok(SchemaKey::Glue(version)),
            SchemaId::Key(SchemaKey::Confluent(_)) | SchemaId::ConfluentGuid(_) => {
                Err(Failure::Rejected(Rejection::OtherRegistry))
            }
        }
    }

    async fn schema(&self, key: SchemaKey) -> Result<RegistrySchema, Failure> {
        let SchemaKey::Glue(version) = key else {
            return Err(Failure::Failed(Error::ForeignSchemaKey(key)));
        };

        let output = self.version(version).await?;
        let Some(format) = output.data_format().and_then(format) else {
            return Err(Failure::Rejected(Rejection::Unsupported));
        };
        let Some(definition) = output.schema_definition() else {
            return Err(Failure::Rejected(Rejection::Missing));
        };

        Ok(RegistrySchema::new(key, format, definition, Vec::new()))
    }

    async fn accepted(&self, topic: &str, _part: Part, key: SchemaKey) -> Result<bool, Failure> {
        let SchemaKey::Glue(version) = key else {
            return Err(Failure::Failed(Error::ForeignSchemaKey(key)));
        };

        let output = self.version(version).await?;

        Ok(output.schema_arn().and_then(schema_name) == Some((self.name.as_str(), topic)))
    }
}

fn format(format: &DataFormat) -> Option<SchemaFormat> {
    match format {
        DataFormat::Avro => Some(SchemaFormat::Avro),
        DataFormat::Json => Some(SchemaFormat::Json),
        DataFormat::Protobuf => Some(SchemaFormat::Protobuf),
        _ => None,
    }
}

fn schema_name(arn: &str) -> Option<(&str, &str)> {
    let (_, path) = arn.rsplit_once(":schema/")?;

    path.split_once('/')
}

fn missing(error: &SdkError<GetSchemaVersionError>) -> bool {
    error
        .as_service_error()
        .is_some_and(GetSchemaVersionError::is_entity_not_found_exception)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn versions_resolve_to_themselves_and_confluent_ids_to_another_registry() {
        let region = Some("us-east-1".to_owned());
        let glue = Glue::new("mink".to_owned(), region, Metrics::new()).await;
        let version = Uuid::new_v4();

        let own = glue.resolve(SchemaId::Key(SchemaKey::Glue(version))).await;
        let id = glue.resolve(SchemaId::Key(SchemaKey::Confluent(7))).await;
        let guid = glue.resolve(SchemaId::ConfluentGuid(version)).await;

        assert!(
            matches!(own, Ok(key) if key == SchemaKey::Glue(version)),
            "{own:?}"
        );
        for other in [id, guid] {
            assert!(
                matches!(other, Err(Failure::Rejected(Rejection::OtherRegistry))),
                "{other:?}"
            );
        }
    }

    #[test]
    fn glue_data_formats_map_to_schema_formats() {
        assert_eq!(format(&DataFormat::Avro), Some(SchemaFormat::Avro));
        assert_eq!(format(&DataFormat::Json), Some(SchemaFormat::Json));
        assert_eq!(format(&DataFormat::Protobuf), Some(SchemaFormat::Protobuf));
        assert_eq!(format(&DataFormat::from("XML")), None);
    }

    #[test]
    fn a_schema_arn_names_its_registry_and_schema() {
        let arn = "arn:aws:glue:us-east-1:855441379890:schema/mink-fixtures/orders";

        assert_eq!(schema_name(arn), Some(("mink-fixtures", "orders")));
        assert_eq!(schema_name("arn:aws:glue:us-east-1:1:registry/mink"), None);
    }
}
