//! Topic administration: create and delete topics, and describe the configuration keys honored
//! (`retention.ms`, `cleanup.policy`).

use std::time::Duration;

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::{
    CreatableTopicConfigs, CreatableTopicResult,
};
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult,
};
use kafka_protocol::messages::{
    CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse,
    DescribeConfigsRequest, DescribeConfigsResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use mink_metadata::View;

use crate::Kafka;
use crate::dispatch::Request;
use crate::error::Error;
use crate::topic;

const RETENTION_MS: &str = "retention.ms";
const CLEANUP_POLICY: &str = "cleanup.policy";
const TOPIC_RESOURCE: i8 = 2;
const DEFAULT_CONFIG_SOURCE: i8 = 5;
const LONG_TYPE: i8 = 5;
const LIST_TYPE: i8 = 7;

struct Spec {
    partitions: u32,
    retention: Option<Duration>,
}

fn spec(topic: &CreatableTopic, default_partitions: u32) -> Result<Spec, Error> {
    let partitions = match (topic.num_partitions, topic.assignments.len()) {
        (-1, 0) => default_partitions,
        (-1, assigned) => assigned as u32,
        (n, _) if n > 0 => n as u32,
        (n, _) => {
            return Err(Error::InvalidPartitions(format!(
                "num_partitions {n} must be positive"
            )));
        }
    };
    if topic.replication_factor < -1 || topic.replication_factor == 0 {
        return Err(Error::InvalidRequest(format!(
            "replication_factor {} must be -1 or positive",
            topic.replication_factor
        )));
    }

    let mut retention = None;
    for config in &topic.configs {
        let value = config.value.as_ref().map(|v| v.as_str());
        match (config.name.as_str(), value) {
            (RETENTION_MS, Some(ms)) => {
                let ms: i64 = ms.parse().map_err(|_| {
                    Error::InvalidConfig(format!("{RETENTION_MS}={ms} is not a number"))
                })?;
                if ms > 0 {
                    retention = Some(Duration::from_millis(ms as u64));
                }
            }
            (CLEANUP_POLICY, Some(policy)) if policy != "delete" => {
                return Err(Error::InvalidConfig(format!(
                    "{CLEANUP_POLICY}={policy} is not supported; only delete"
                )));
            }
            _ => {}
        }
    }

    Ok(Spec {
        partitions,
        retention,
    })
}

fn configs(retention: Option<Duration>) -> Vec<CreatableTopicConfigs> {
    vec![
        CreatableTopicConfigs::default()
            .with_name(StrBytes::from_static_str(RETENTION_MS))
            .with_value(Some(StrBytes::from(retention_ms(retention))))
            .with_config_source(DEFAULT_CONFIG_SOURCE),
        CreatableTopicConfigs::default()
            .with_name(StrBytes::from_static_str(CLEANUP_POLICY))
            .with_value(Some(StrBytes::from_static_str("delete")))
            .with_config_source(DEFAULT_CONFIG_SOURCE),
    ]
}

fn retention_ms(retention: Option<Duration>) -> String {
    retention.map_or_else(|| "-1".to_owned(), |d| d.as_millis().to_string())
}

fn message(error: &Error) -> Option<StrBytes> {
    Some(StrBytes::from(error.to_string()))
}

impl Kafka {
    pub(crate) async fn create_topics(
        &self,
        _request: &Request,
        create: CreateTopicsRequest,
    ) -> Result<CreateTopicsResponse, Error> {
        let mut results = Vec::with_capacity(create.topics.len());
        for wanted in &create.topics {
            let mut result = CreatableTopicResult::default()
                .with_name(wanted.name.clone())
                .with_replication_factor(1);
            result = match spec(wanted, self.config().default_partitions) {
                Err(e) => result
                    .with_error_code(e.code())
                    .with_error_message(message(&e)),
                Ok(spec) if create.validate_only => {
                    match topic::path(&self.config().database, wanted.name.as_str()) {
                        Ok(_) => result
                            .with_num_partitions(spec.partitions as i32)
                            .with_configs(Some(configs(spec.retention))),
                        Err(e) => result
                            .with_error_code(e.code())
                            .with_error_message(message(&e)),
                    }
                }
                Ok(spec) => {
                    match self
                        .create_topic(wanted.name.as_str(), spec.partitions, spec.retention)
                        .await
                    {
                        Ok(topic) => result
                            .with_topic_id(topic.uuid())
                            .with_num_partitions(topic.partitions as i32)
                            .with_configs(Some(configs(spec.retention))),
                        Err(e) => result
                            .with_error_code(e.code())
                            .with_error_message(message(&e)),
                    }
                }
            };
            results.push(result);
        }

        Ok(CreateTopicsResponse::default().with_topics(results))
    }

    pub(crate) async fn delete_topics(
        &self,
        request: &Request,
        delete: DeleteTopicsRequest,
    ) -> Result<DeleteTopicsResponse, Error> {
        let mut wanted: Vec<(Option<TopicName>, uuid::Uuid)> = if request.version >= 6 {
            delete
                .topics
                .into_iter()
                .map(|t| (t.name, t.topic_id))
                .collect()
        } else {
            Vec::new()
        };
        wanted.extend(
            delete
                .topic_names
                .into_iter()
                .map(|name| (Some(name), uuid::Uuid::nil())),
        );

        let mut results = Vec::with_capacity(wanted.len());
        for (name, id) in wanted {
            let view = self.inner.service.view();
            let resolved = match &name {
                Some(name) => self.topic(&view, name.as_str()),
                None => self.topic_by_id(&view, id),
            };
            let result = DeletableTopicResult::default()
                .with_name(name.clone())
                .with_topic_id(id);
            let outcome = match resolved {
                Ok(topic) => self.delete_topic(&topic.name).await.map(|()| topic),
                Err(e) => Err(e),
            };
            results.push(match outcome {
                Ok(topic) => {
                    let id = topic.uuid();
                    result
                        .with_name(Some(TopicName(StrBytes::from(topic.name))))
                        .with_topic_id(id)
                }
                Err(e) => result
                    .with_error_code(e.code())
                    .with_error_message(message(&e)),
            });
        }

        Ok(DeleteTopicsResponse::default().with_responses(results))
    }

    pub(crate) async fn describe_configs(
        &self,
        _request: &Request,
        describe: DescribeConfigsRequest,
    ) -> Result<DescribeConfigsResponse, Error> {
        let view = self.inner.service.view();
        let results = describe
            .resources
            .into_iter()
            .map(|resource| {
                let result = DescribeConfigsResult::default()
                    .with_resource_type(resource.resource_type)
                    .with_resource_name(resource.resource_name.clone());
                if resource.resource_type != TOPIC_RESOURCE {
                    return result;
                }
                match self.topic_configs(&view, resource.resource_name.as_str()) {
                    Ok(configs) => result.with_configs(
                        configs
                            .into_iter()
                            .filter(|c| {
                                resource
                                    .configuration_keys
                                    .as_ref()
                                    .is_none_or(|keys| keys.contains(&c.name))
                            })
                            .collect(),
                    ),
                    Err(e) => result
                        .with_error_code(e.code())
                        .with_error_message(message(&e)),
                }
            })
            .collect();

        Ok(DescribeConfigsResponse::default().with_results(results))
    }

    fn topic_configs(
        &self,
        view: &View,
        name: &str,
    ) -> Result<Vec<DescribeConfigsResourceResult>, Error> {
        let path = topic::path(&self.config().database, name)?;
        let row = view
            .state
            .catalog
            .tables
            .get(&path)
            .ok_or_else(|| Error::UnknownTopic(name.to_owned()))?;
        let retention = row.descriptor.options().log_ttl;

        Ok(vec![
            DescribeConfigsResourceResult::default()
                .with_name(StrBytes::from_static_str(RETENTION_MS))
                .with_value(Some(StrBytes::from(retention_ms(retention))))
                .with_config_source(DEFAULT_CONFIG_SOURCE)
                .with_config_type(LONG_TYPE),
            DescribeConfigsResourceResult::default()
                .with_name(StrBytes::from_static_str(CLEANUP_POLICY))
                .with_value(Some(StrBytes::from_static_str("delete")))
                .with_read_only(true)
                .with_config_source(DEFAULT_CONFIG_SOURCE)
                .with_config_type(LIST_TYPE),
        ])
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::create_topics_request::CreatableTopicConfig;

    use super::*;

    fn topic(partitions: i32, configs: &[(&str, &str)]) -> CreatableTopic {
        CreatableTopic::default()
            .with_num_partitions(partitions)
            .with_replication_factor(-1)
            .with_configs(
                configs
                    .iter()
                    .map(|(k, v)| {
                        CreatableTopicConfig::default()
                            .with_name(StrBytes::from(k.to_string()))
                            .with_value(Some(StrBytes::from(v.to_string())))
                    })
                    .collect(),
            )
    }

    #[test]
    fn specs_take_defaults_and_honor_retention() {
        let s = spec(&topic(-1, &[]), 3).unwrap();
        assert_eq!((s.partitions, s.retention), (3, None));
        let s = spec(&topic(4, &[(RETENTION_MS, "60000")]), 3).unwrap();
        assert_eq!(
            (s.partitions, s.retention),
            (4, Some(Duration::from_millis(60_000)))
        );
    }

    #[test]
    fn compaction_and_bad_counts_are_rejected() {
        assert!(matches!(
            spec(&topic(1, &[(CLEANUP_POLICY, "compact")]), 1),
            Err(Error::InvalidConfig(_))
        ));
        assert!(matches!(
            spec(&topic(0, &[]), 1),
            Err(Error::InvalidPartitions(_))
        ));
        assert!(matches!(
            spec(&topic(1, &[(RETENTION_MS, "soon")]), 1),
            Err(Error::InvalidConfig(_))
        ));
    }
}
