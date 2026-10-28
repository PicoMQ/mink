mod http;
mod tables;
mod tail;

pub(crate) use http::router;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mink_core::{DaySketches, Scope, Topic};
    use reqwest::StatusCode;
    use serde_json::Value;
    use uuid::Uuid;

    use crate::testing::{self, TTL, kafka, registered};
    use crate::writer::Writer;
    use crate::{Node, Role};

    async fn catalog() -> Option<(Node, String)> {
        let bootstrap = kafka()?;
        let mut config = testing::config(&[Role::Catalog, Role::S3]).await?;
        config.s3_endpoint = "http://mink-s3.test:9100".to_owned();

        Some((testing::node_with(config).await, bootstrap))
    }

    async fn committed(node: &Node, bootstrap: &str, records: usize) -> Topic {
        let services = &node.services;
        let topic = registered(services, 1).await;
        mink_conformance::create_topic(bootstrap, topic.name(), 1).await;
        let values: Vec<String> = (0..records).map(|index| format!("v{index}")).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();
        mink_conformance::produce_values(bootstrap, topic.name(), 0, &values);

        let indexer = testing::partition_lease(services, &topic, 0).await;
        services
            .store
            .indexed_ranges()
            .start(&indexer, 0)
            .await
            .unwrap();
        let file = mink_conformance::file(topic.name(), 0, 0, records as i64);
        services
            .store
            .files()
            .add(
                &indexer,
                &mink_conformance::built(&[file]),
                &DaySketches::default(),
            )
            .await
            .unwrap();
        let scope = Scope::Topic(topic.name().to_owned());
        let lease = services
            .store
            .leases()
            .acquire(&scope, Uuid::from_u128(1), TTL)
            .await
            .unwrap()
            .unwrap();
        let mut writer = Writer::start(services.clone(), lease, topic.name().to_owned())
            .await
            .unwrap();
        writer.tick().await.unwrap();

        topic
    }

    fn url(node: &Node, path: &str) -> String {
        format!("http://{}{path}", node.catalog_address().unwrap())
    }

    async fn get(node: &Node, path: &str) -> (StatusCode, Value) {
        let response = reqwest::get(url(node, path)).await.unwrap();

        (response.status(), response.json().await.unwrap())
    }

    #[tokio::test]
    async fn config_advertises_the_read_routes_and_the_s3_endpoint() {
        let Some((node, _)) = catalog().await else {
            return;
        };

        let (status, config) = get(&node, "/v1/config").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            config["defaults"]["s3.endpoint"],
            "http://mink-s3.test:9100"
        );
        assert_eq!(config["defaults"]["s3.path-style-access"], "true");
        assert!(
            config["endpoints"]
                .as_array()
                .unwrap()
                .contains(&Value::from(
                    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
                ))
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn namespaces_and_tables_list_the_registered_topics() {
        let Some((node, bootstrap)) = catalog().await else {
            return;
        };
        let topic = committed(&node, &bootstrap, 3).await;
        let unsnapshotted = registered(&node.services, 1).await;

        let (_, namespaces) = get(&node, "/v1/namespaces").await;
        let (_, tables) = get(&node, "/v1/namespaces/kafka/tables").await;
        let (missing, _) = get(&node, "/v1/namespaces/other/tables").await;

        let names: Vec<_> = tables["identifiers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|identifier| identifier["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(namespaces["namespaces"], serde_json::json!([["kafka"]]));
        assert!(names.contains(&topic.name().to_owned()));
        assert!(!names.contains(&unsnapshotted.name().to_owned()));
        assert_eq!(missing, StatusCode::NOT_FOUND);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_table_without_a_snapshot_is_not_found() {
        let Some((node, _)) = catalog().await else {
            return;
        };
        let topic = registered(&node.services, 1).await;

        let (status, error) = get(
            &node,
            &format!("/v1/namespaces/kafka/tables/{}", topic.name()),
        )
        .await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(error["error"]["type"], "NoSuchTableException");
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_load_with_nothing_new_returns_the_committed_metadata() {
        let Some((node, bootstrap)) = catalog().await else {
            return;
        };
        let topic = committed(&node, &bootstrap, 3).await;

        let (status, loaded) = get(
            &node,
            &format!("/v1/namespaces/kafka/tables/{}", topic.name()),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(
            loaded["metadata-location"]
                .as_str()
                .unwrap()
                .ends_with("/metadata/v1-e1.metadata.json")
        );
        assert_eq!(loaded["metadata"]["current-snapshot-id"], 1);
        assert_eq!(loaded["config"]["s3.endpoint"], "http://mink-s3.test:9100");
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn loads_of_the_same_offsets_share_one_tail() {
        let Some((node, bootstrap)) = catalog().await else {
            return;
        };
        let topic = committed(&node, &bootstrap, 3).await;
        mink_conformance::produce_values(&bootstrap, topic.name(), 0, &["d", "e"]);
        let path = format!("/v1/namespaces/kafka/tables/{}", topic.name());

        let (status, first) = get(&node, &path).await;
        let (_, second) = get(&node, &path).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        let location = first["metadata-location"].as_str().unwrap();
        let summary = &first["metadata"]["snapshots"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["summary"];
        let unreferenced = node
            .services
            .store
            .objects()
            .collectable(topic.name(), Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(location.contains("/metadata/v1-e1-t"), "{location}");
        assert_eq!(summary["mink.partition.0.end-offset"], "5");
        assert_eq!(summary["total-records"], "5");
        assert_eq!(first, second);
        assert_eq!(unreferenced.len(), 3);
        node.shutdown().await.unwrap();
    }
}
