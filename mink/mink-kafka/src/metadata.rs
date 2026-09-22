//! Cluster and topic metadata: brokers are nodes with a Kafka address, the controller and group
//! coordinator are the coordinator node, and unknown topics are created on request.

use kafka_protocol::ResponseError;
use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{
    BrokerId, DescribeClusterRequest, DescribeClusterResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, MetadataRequest, MetadataResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use mink_metadata::View;

use crate::dispatch::Request;
use crate::error::Error;
use crate::topic::Topic;
use crate::{Kafka, PROTOCOL};

pub(crate) struct Broker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

pub(crate) fn brokers(view: &View) -> Vec<Broker> {
    view.state
        .nodes
        .iter()
        .filter_map(|(id, _)| broker(view, *id))
        .collect()
}

pub(crate) fn broker(view: &View, node_id: i32) -> Option<Broker> {
    let address = view.state.get_node_protocol_address(node_id, PROTOCOL)?;
    let (host, port) = split(address);

    Some(Broker {
        node_id,
        host: host.to_owned(),
        port,
    })
}

pub(crate) fn coordinator(view: &View) -> Option<Broker> {
    view.state
        .catalog
        .coordinator
        .as_ref()
        .and_then(|row| broker(view, row.node_id))
}

fn split(address: &str) -> (&str, i32) {
    let address = address.split_once("://").map_or(address, |(_, rest)| rest);
    if let Some(rest) = address.strip_prefix('[')
        && let Some((host, port)) = rest.split_once("]:")
    {
        return (host, port.parse().unwrap_or(9092));
    }

    match address.rsplit_once(':') {
        Some((host, port)) => (host, port.parse().unwrap_or(9092)),
        None => (address, 9092),
    }
}

fn str(s: &str) -> StrBytes {
    StrBytes::from(s.to_owned())
}

impl Kafka {
    pub(crate) async fn metadata(
        &self,
        _request: &Request,
        request: MetadataRequest,
    ) -> Result<MetadataResponse, Error> {
        let auto_create = request.allow_auto_topic_creation && self.config().auto_create_topics;
        let mut view = self.inner.service.view();
        let mut topics = Vec::new();
        match request.topics {
            None => {
                for topic in self.topics(&view) {
                    topics.push(self.describe_topic(&view, topic));
                }
            }
            Some(requested) => {
                for wanted in requested {
                    let found = match &wanted.name {
                        Some(name) => self.topic(&view, name.as_str()),
                        None => self.topic_by_id(&view, wanted.topic_id),
                    };
                    let described = match (found, &wanted.name) {
                        (Ok(topic), _) => self.describe_topic(&view, topic),
                        (Err(Error::UnknownTopic(_)), Some(name)) if auto_create => {
                            match self
                                .create_topic(name.as_str(), self.config().default_partitions, None)
                                .await
                            {
                                Ok(topic) => {
                                    view = self.inner.service.view();
                                    self.describe_topic(&view, topic)
                                }
                                Err(e) => failed_topic(&wanted.name, wanted.topic_id, &e),
                            }
                        }
                        (Err(e), _) => failed_topic(&wanted.name, wanted.topic_id, &e),
                    };
                    topics.push(described);
                }
            }
        }

        let brokers = brokers(&view)
            .into_iter()
            .map(|b| {
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId(b.node_id))
                    .with_host(str(&b.host))
                    .with_port(b.port)
            })
            .collect();

        Ok(MetadataResponse::default()
            .with_brokers(brokers)
            .with_cluster_id(Some(str(&self.inner.cluster_id)))
            .with_controller_id(BrokerId(coordinator(&view).map_or(-1, |b| b.node_id)))
            .with_topics(topics)
            .with_cluster_authorized_operations(-2147483648))
    }

    fn describe_topic(&self, view: &View, topic: Topic) -> MetadataResponseTopic {
        let partitions = topic
            .buckets()
            .map(|(index, bucket)| {
                let partition = MetadataResponsePartition::default().with_partition_index(index);
                match self.leader(view, bucket) {
                    Some((leader, epoch)) if broker(view, leader).is_some() => partition
                        .with_leader_id(BrokerId(leader))
                        .with_leader_epoch(epoch)
                        .with_replica_nodes(vec![BrokerId(leader)])
                        .with_isr_nodes(vec![BrokerId(leader)]),
                    _ => partition
                        .with_error_code(ResponseError::LeaderNotAvailable.code())
                        .with_leader_id(BrokerId(-1))
                        .with_leader_epoch(-1),
                }
            })
            .collect();

        MetadataResponseTopic::default()
            .with_name(Some(TopicName(str(&topic.name))))
            .with_topic_id(topic.uuid())
            .with_partitions(partitions)
            .with_topic_authorized_operations(-2147483648)
    }

    pub(crate) async fn describe_cluster(
        &self,
        _request: &Request,
        _cluster: DescribeClusterRequest,
    ) -> Result<DescribeClusterResponse, Error> {
        let view = self.inner.service.view();
        let brokers = brokers(&view)
            .into_iter()
            .map(|b| {
                DescribeClusterBroker::default()
                    .with_broker_id(BrokerId(b.node_id))
                    .with_host(str(&b.host))
                    .with_port(b.port)
            })
            .collect();

        Ok(DescribeClusterResponse::default()
            .with_endpoint_type(1)
            .with_cluster_id(str(&self.inner.cluster_id))
            .with_controller_id(BrokerId(coordinator(&view).map_or(-1, |b| b.node_id)))
            .with_brokers(brokers)
            .with_cluster_authorized_operations(-2147483648))
    }

    pub(crate) async fn find_coordinator(
        &self,
        request: &Request,
        find: FindCoordinatorRequest,
    ) -> Result<FindCoordinatorResponse, Error> {
        let view = self.inner.service.view();
        let located = match find.key_type {
            0 => coordinator(&view).ok_or(ResponseError::CoordinatorNotAvailable),
            _ => Err(ResponseError::InvalidRequest),
        };
        let mut response = FindCoordinatorResponse::default();
        if request.version >= 4 {
            let coordinators = find
                .coordinator_keys
                .iter()
                .map(|key| {
                    let entry = Coordinator::default().with_key(key.clone());
                    match &located {
                        Ok(b) => entry
                            .with_node_id(BrokerId(b.node_id))
                            .with_host(str(&b.host))
                            .with_port(b.port),
                        Err(e) => entry
                            .with_error_code(e.code())
                            .with_node_id(BrokerId(-1))
                            .with_port(-1),
                    }
                })
                .collect();
            response = response.with_coordinators(coordinators);
        } else {
            response = match &located {
                Ok(b) => response
                    .with_node_id(BrokerId(b.node_id))
                    .with_host(str(&b.host))
                    .with_port(b.port),
                Err(e) => response
                    .with_error_code(e.code())
                    .with_node_id(BrokerId(-1))
                    .with_port(-1),
            };
        }

        Ok(response)
    }
}

fn failed_topic(name: &Option<TopicName>, id: uuid::Uuid, error: &Error) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .with_error_code(error.code())
        .with_name(name.clone())
        .with_topic_id(id)
        .with_topic_authorized_operations(-2147483648)
}

#[cfg(test)]
mod tests {
    use super::split;

    #[test]
    fn addresses_split_into_host_and_port() {
        assert_eq!(split("broker-1:9092"), ("broker-1", 9092));
        assert_eq!(split("kafka://10.0.0.5:19092"), ("10.0.0.5", 19092));
        assert_eq!(split("[::1]:9093"), ("::1", 9093));
        assert_eq!(split("nohost"), ("nohost", 9092));
    }
}
