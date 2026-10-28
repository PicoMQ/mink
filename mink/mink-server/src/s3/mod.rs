mod build;
mod cache;
mod http;
mod key;
mod origin;
mod reader;

pub(crate) use cache::Cache;
pub(crate) use http::router;

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use mink_core::{
        DaySketches, File, IndexedFile, Member, Partition, Stats, TableLocation, TimePartition,
        Topic,
    };
    use reqwest::StatusCode;
    use uuid::Uuid;

    use crate::Role;
    use crate::testing::{self, kafka, node, registered};

    fn url(node: &crate::Node, path: &str) -> String {
        format!(
            "http://{}/mink/{}",
            node.s3_address().unwrap(),
            path.strip_prefix("s3://mink/").unwrap()
        )
    }

    #[tokio::test]
    async fn metadata_objects_pass_through_from_the_warehouse() {
        let Some(node) = node(&[Role::S3]).await else {
            return;
        };
        let topic = registered(&node.services, 1).await;
        let path = format!(
            "s3://mink/warehouse/{}/metadata/v1-e1.metadata.json",
            topic.name()
        );
        let unregistered = path.replace(topic.name(), "unregistered");
        node.services
            .warehouse
            .write(&path, Bytes::from_static(b"{}"))
            .await
            .unwrap();

        let found = reqwest::get(url(&node, &path)).await.unwrap();
        let missing = reqwest::get(url(&node, &unregistered)).await.unwrap();

        assert_eq!(found.status(), StatusCode::OK);
        assert_eq!(found.bytes().await.unwrap(), Bytes::from_static(b"{}"));
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn data_files_are_built_from_kafka_and_served_by_range() {
        let (Some(bootstrap), Some(node)) = (kafka(), node(&[Role::S3]).await) else {
            return;
        };
        let topic = registered(&node.services, 1).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
        let values: Vec<String> = (0..10).map(|index| format!("v{index}")).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();
        mink_conformance::produce_values(&bootstrap, topic.name(), 0, &values);
        let file = File::v1(
            Partition::new(topic.name(), 0).unwrap(),
            0,
            10,
            TimePartition::new(0, None),
            0,
        )
        .unwrap();
        let path = TableLocation::data_file("s3://mink/warehouse", &file);
        let expected = super::build::build(&node.services, &file).await.unwrap();

        let client = reqwest::Client::new();
        let whole = client.get(url(&node, &path)).send().await.unwrap();
        let ranged = client
            .get(url(&node, &path))
            .header("range", "bytes=-4")
            .send()
            .await
            .unwrap();

        assert_eq!(whole.status(), StatusCode::OK);
        assert_eq!(whole.bytes().await.unwrap(), expected);
        assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(ranged.bytes().await.unwrap(), Bytes::from_static(b"PAR1"));
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn head_takes_the_length_from_stored_stats() {
        let Some(node) = node(&[Role::S3]).await else {
            return;
        };
        let topic = registered(&node.services, 1).await;
        let lease = testing::partition_lease(&node.services, &topic, 0).await;
        let file = File::v1(
            Partition::new(topic.name(), 0).unwrap(),
            0,
            10,
            TimePartition::new(0, None),
            0,
        )
        .unwrap();
        node.services
            .store
            .indexed_ranges()
            .start(&lease, 0)
            .await
            .unwrap();
        node.services
            .store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[IndexedFile::new(
                    file.clone(),
                    Stats::new(10, 12345, 0),
                )]),
                &DaySketches::default(),
            )
            .await
            .unwrap();

        let head = reqwest::Client::new()
            .head(url(
                &node,
                &TableLocation::data_file("s3://mink/warehouse", &file),
            ))
            .send()
            .await
            .unwrap();

        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(head.headers()["content-length"], "12345");
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_length_that_differs_from_the_stats_is_not_served() {
        let (Some(bootstrap), Some(node)) = (kafka(), node(&[Role::S3]).await) else {
            return;
        };
        let topic = registered(&node.services, 1).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
        mink_conformance::produce_values(&bootstrap, topic.name(), 0, &["a", "b", "c"]);
        let lease = testing::partition_lease(&node.services, &topic, 0).await;
        let file = File::v1(
            Partition::new(topic.name(), 0).unwrap(),
            0,
            3,
            TimePartition::new(0, None),
            0,
        )
        .unwrap();
        node.services
            .store
            .indexed_ranges()
            .start(&lease, 0)
            .await
            .unwrap();
        node.services
            .store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[IndexedFile::new(file.clone(), Stats::new(3, 1, 0))]),
                &DaySketches::default(),
            )
            .await
            .unwrap();

        let response = reqwest::get(url(
            &node,
            &TableLocation::data_file("s3://mink/warehouse", &file),
        ))
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unknown_keys_and_writes_are_rejected() {
        let Some(node) = node(&[Role::S3]).await else {
            return;
        };
        let client = reqwest::Client::new();

        let unknown = client
            .get(url(&node, "s3://mink/warehouse/nothing/here"))
            .send()
            .await
            .unwrap();
        let other_bucket = client
            .get(format!("http://{}/other/key", node.s3_address().unwrap()))
            .send()
            .await
            .unwrap();
        let write = client
            .put(url(&node, "s3://mink/warehouse/orders/metadata/x"))
            .body("x")
            .send()
            .await
            .unwrap();

        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        assert_eq!(other_bucket.status(), StatusCode::NOT_FOUND);
        assert_eq!(write.status(), StatusCode::FORBIDDEN);
        node.shutdown().await.unwrap();
    }

    async fn s3_nodes() -> Option<(crate::Node, crate::Node)> {
        let mut config = testing::config(&[Role::S3]).await?;
        config.lease_ttl = std::time::Duration::from_secs(60);

        let first = testing::node_with(config.clone()).await;
        Some((first, testing::node_with(config).await))
    }

    fn served_by(response: &reqwest::Response) -> String {
        response.headers()["mink-served-by"]
            .to_str()
            .unwrap()
            .to_owned()
    }

    #[tokio::test]
    async fn reads_are_forwarded_to_the_owner_once() {
        let (Some(bootstrap), Some((first, second))) = (kafka(), s3_nodes().await) else {
            return;
        };
        let topic = registered(&first.services, 1).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
        let values: Vec<String> = (0..40).map(|index| format!("v{index}")).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();
        mink_conformance::produce_values(&bootstrap, topic.name(), 0, &values);

        let unreachable = Member::new(
            Uuid::new_v4(),
            vec![Role::S3],
            Some("http://127.0.0.1:9".to_owned()),
            None,
        );
        let members = first.services.store.members();
        members
            .register(&unreachable, std::time::Duration::from_secs(60))
            .await
            .unwrap();
        let first_member = Member::new(
            first.services.membership.id(),
            vec![Role::S3],
            Some(format!("http://{}", first.s3_address().unwrap())),
            None,
        );
        first
            .services
            .membership
            .renew(
                &first.services.store,
                &first_member,
                first.services.config.lease_ttl,
            )
            .await
            .unwrap();

        let partition = Partition::new(topic.name(), 0).unwrap();
        let files: Vec<String> = (0..40)
            .flat_map(|start| (start + 1..=40).map(move |end| (start, end)))
            .map(|(start, end)| {
                let file = File::v1(
                    partition.clone(),
                    start,
                    end,
                    TimePartition::new(0, None),
                    0,
                )
                .unwrap();

                TableLocation::data_file("s3://mink/warehouse", &file)
            })
            .collect();
        let owned_by = |owner: Uuid| {
            files
                .iter()
                .find(|path| {
                    first
                        .services
                        .membership
                        .remote_owner(path)
                        .is_some_and(|node| node.id() == owner)
                })
                .unwrap()
        };
        let (first_id, second_id) = (
            first.services.membership.id().to_string(),
            second.services.membership.id().to_string(),
        );
        let (at_second, at_nobody) = (
            owned_by(second.services.membership.id()),
            owned_by(unreachable.id()),
        );
        let client = reqwest::Client::new();

        let forwarded = client.get(url(&first, at_second)).send().await.unwrap();
        let ranged = client
            .get(url(&first, at_second))
            .header("range", "bytes=0-3")
            .send()
            .await
            .unwrap();
        let headed = client.head(url(&first, at_second)).send().await.unwrap();
        let direct = client.get(url(&second, at_second)).send().await.unwrap();
        let not_again = client
            .get(url(&first, at_second))
            .header("mink-forwarded-by", second_id.clone())
            .send()
            .await
            .unwrap();
        let fallback = client.get(url(&first, at_nobody)).send().await.unwrap();
        let built = builds(&first);
        let fallback_again = client.get(url(&first, at_nobody)).send().await.unwrap();

        assert_eq!(forwarded.status(), StatusCode::OK);
        assert_eq!(served_by(&forwarded), second_id);
        assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(served_by(&ranged), second_id);
        assert_eq!(served_by(&headed), second_id);
        assert_eq!(served_by(&direct), second_id);
        assert_eq!(
            forwarded.bytes().await.unwrap(),
            direct.bytes().await.unwrap()
        );
        assert_eq!(served_by(&not_again), first_id);
        assert_eq!(fallback.status(), StatusCode::OK);
        assert_eq!(served_by(&fallback), first_id);
        assert_eq!(fallback_again.status(), StatusCode::OK);
        assert_eq!(builds(&first), built + 1);

        members.remove(unreachable.id()).await.unwrap();
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
    }

    fn builds(node: &crate::Node) -> u64 {
        node.services
            .metrics
            .render()
            .lines()
            .find_map(|line| line.strip_prefix(r#"mink_builds_total{result="ok"} "#))
            .and_then(|count| count.parse().ok())
            .unwrap()
    }

    async fn produced(bootstrap: &str, node: &crate::Node, records: usize) -> Topic {
        let topic = registered(&node.services, 1).await;
        mink_conformance::create_topic(bootstrap, topic.name(), 1).await;
        let values: Vec<String> = (0..records).map(|index| format!("v{index}")).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();
        mink_conformance::produce_values(bootstrap, topic.name(), 0, &values);

        topic
    }

    async fn large(bootstrap: &str, node: &crate::Node, records: usize) -> Topic {
        let topic = registered(&node.services, 1).await;
        mink_conformance::create_topic(bootstrap, topic.name(), 1).await;
        let values: Vec<String> = (0..records)
            .map(|_| {
                (0..3000)
                    .map(|_| Uuid::new_v4().simple().to_string())
                    .collect()
            })
            .collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();
        mink_conformance::produce_values(bootstrap, topic.name(), 0, &values);

        topic
    }

    fn data_file(topic: &str, start: i64, end: i64) -> (File, String) {
        let file = File::v1(
            Partition::new(topic, 0).unwrap(),
            start,
            end,
            TimePartition::new(0, None),
            0,
        )
        .unwrap();
        let path = TableLocation::data_file("s3://mink/warehouse", &file);

        (file, path)
    }

    async fn get(node: &crate::Node, path: &str, range: Option<&str>) -> reqwest::Response {
        let mut request = reqwest::Client::new().get(url(node, path));
        if let Some(range) = range {
            request = request.header("range", range);
        }

        request.send().await.unwrap()
    }

    #[tokio::test]
    async fn a_footer_read_builds_the_file_once_and_caches_all_of_it() {
        let (Some(bootstrap), Some(node)) = (kafka(), node(&[Role::S3]).await) else {
            return;
        };
        let topic = large(&bootstrap, &node, 60).await;
        let (_, path) = data_file(topic.name(), 0, 60);
        let before = builds(&node);

        let footer = get(&node, &path, Some("bytes=-8")).await;
        let block = get(&node, &path, Some("bytes=1048576-1048583")).await;
        let head = reqwest::Client::new()
            .head(url(&node, &path))
            .send()
            .await
            .unwrap();
        let whole = get(&node, &path, None).await.bytes().await.unwrap();

        assert!(whole.len() > 2 * 1024 * 1024, "{}", whole.len());
        assert_eq!(footer.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            footer.bytes().await.unwrap(),
            whole.slice(whole.len() - 8..)
        );
        assert_eq!(block.bytes().await.unwrap(), whole.slice(1048576..1048584));
        assert_eq!(head.headers()["content-length"], whole.len().to_string());
        assert_eq!(builds(&node), before + 1);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_range_past_the_end_is_416_and_a_malformed_one_reads_the_whole_file() {
        let (Some(bootstrap), Some(node)) = (kafka(), node(&[Role::S3]).await) else {
            return;
        };
        let topic = produced(&bootstrap, &node, 40).await;
        let (_, path) = data_file(topic.name(), 0, 40);
        let whole = get(&node, &path, None).await.bytes().await.unwrap();

        let past = get(&node, &path, Some(&format!("bytes={}-", whole.len()))).await;
        let malformed = get(&node, &path, Some("bytes=0-1,4-5")).await;

        assert_eq!(past.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            past.headers()["content-range"],
            format!("bytes */{}", whole.len())
        );
        assert_eq!(malformed.status(), StatusCode::OK);
        assert_eq!(malformed.bytes().await.unwrap(), whole);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cached_files_are_served_while_postgres_is_down() {
        let (Some(bootstrap), Some(node)) = (kafka(), node(&[Role::S3]).await) else {
            return;
        };
        let topic = produced(&bootstrap, &node, 40).await;
        let (_, cached) = data_file(topic.name(), 0, 20);
        let (_, uncached) = data_file(topic.name(), 20, 40);
        let expected = get(&node, &cached, None).await.bytes().await.unwrap();

        node.services.store.close().await;
        let served = get(&node, &cached, None).await;
        let unavailable = get(&node, &uncached, None).await;

        assert_eq!(served.status(), StatusCode::OK);
        assert_eq!(served.bytes().await.unwrap(), expected);
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn max_builds_limits_the_files_built_at_once() {
        let (Some(bootstrap), Some(mut config)) = (kafka(), testing::config(&[Role::S3]).await)
        else {
            return;
        };
        config.max_builds = 1;
        let node = testing::node_with(config).await;
        let topic = large(&bootstrap, &node, 60).await;
        let cache = node.cache().unwrap().clone();
        let paths: Vec<String> = [(0, 20), (20, 40), (40, 60)]
            .into_iter()
            .map(|(start, end)| data_file(topic.name(), start, end).1)
            .collect();

        let (read, most) = {
            let reads = futures::future::join_all(paths.iter().map(|path| get(&node, path, None)));
            let mut reads = std::pin::pin!(reads);
            let mut most = 0;

            loop {
                tokio::select! {
                    read = &mut reads => break (read, most),
                    () = tokio::time::sleep(std::time::Duration::from_millis(1)) => {
                        most = most.max(cache.usage().building);
                    }
                }
            }
        };

        assert!(read.iter().all(|read| read.status() == StatusCode::OK));
        assert_eq!(most, 1);
        drop(read);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_reads_build_a_file_once() {
        let (Some(bootstrap), Some(node)) = (kafka(), node(&[Role::S3]).await) else {
            return;
        };
        let topic = produced(&bootstrap, &node, 40).await;
        let (_, path) = data_file(topic.name(), 0, 40);
        let before = builds(&node);

        let reads = (0..8).map(|_| get(&node, &path, None));
        let reads = futures::future::join_all(reads).await;

        assert!(reads.iter().all(|read| read.status() == StatusCode::OK));
        assert_eq!(builds(&node), before + 1);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_restarted_node_serves_files_read_twice_from_disk() {
        let (Some(bootstrap), Some(mut config)) = (kafka(), testing::config(&[Role::S3]).await)
        else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        config.cache_dir = Some(dir.path().to_path_buf());
        config.cache_disk_bytes = 256 * 1024 * 1024;
        let first = testing::node_with(config.clone()).await;
        let topic = produced(&bootstrap, &first, 40).await;
        let (_, twice) = data_file(topic.name(), 0, 20);
        let (_, once) = data_file(topic.name(), 20, 40);

        let expected = get(&first, &twice, None).await.bytes().await.unwrap();
        get(&first, &twice, None).await;
        get(&first, &once, None).await;
        first.shutdown().await.unwrap();

        let second = testing::node_with(config).await;
        let before = builds(&second);
        let kept = get(&second, &twice, None).await;
        let after_kept = builds(&second);
        get(&second, &once, None).await;

        assert_eq!(kept.status(), StatusCode::OK);
        assert_eq!(kept.bytes().await.unwrap(), expected);
        assert_eq!(after_kept, before);
        assert_eq!(builds(&second), before + 1);
        second.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_table_added_again_never_reads_the_old_tables_files() {
        let (Some(bootstrap), Some(node)) = (kafka(), node(&[Role::S3]).await) else {
            return;
        };
        let topic = produced(&bootstrap, &node, 40).await;
        let (_, path) = data_file(topic.name(), 0, 40);
        let topics = node.services.store.topics();

        get(&node, &path, None).await;
        let before = builds(&node);
        topics.remove(topic.name()).await.unwrap();
        topics.delete(topic.name()).await.unwrap();
        let again = Topic::new(topic.name(), Uuid::new_v4(), 1).unwrap();
        topics
            .add(&again, node.services.config.granularity)
            .await
            .unwrap();
        let read = get(&node, &path, None).await;

        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(builds(&node), before + 1);
        node.shutdown().await.unwrap();
    }
}
