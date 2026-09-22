//! Read-only queries over the state: streams, objects, node addresses and key-value pages.

use std::ops::Bound;

use bytes::Bytes;
use s3stream::{CompactOperations, S3ObjectMetadata, StreamMetadata};

use crate::state::{State, StreamObjectRow, StreamOffsetKey};

impl State {
    pub fn get_stream(&self, stream_id: u64) -> Option<StreamMetadata> {
        self.streams
            .get(&stream_id)
            .map(|row| row.to_stream_metadata())
    }

    pub fn get_streams(&self, stream_ids: &[u64]) -> Vec<StreamMetadata> {
        stream_ids
            .iter()
            .filter_map(|id| self.get_stream(*id))
            .collect()
    }

    pub fn get_opening_streams(&self, node_id: i32) -> Vec<StreamMetadata> {
        self.opening_by_node
            .range((node_id, 0)..=(node_id, u64::MAX))
            .filter_map(|(&(_, id), ())| self.get_stream(id))
            .collect()
    }

    pub fn placed_count(&self, node_id: i32) -> usize {
        self.placed_by_node
            .range((node_id, 0)..=(node_id, u64::MAX))
            .count()
    }

    pub fn get_node_address(&self, node_id: i32) -> Option<&str> {
        self.nodes
            .get(&node_id)
            .map(|n| n.http_address.as_str())
            .filter(|a| !a.is_empty())
    }

    pub fn get_node_protocol_address(&self, node_id: i32, protocol: &str) -> Option<&str> {
        self.nodes
            .get(&node_id)
            .and_then(|n| n.protocol_addresses.get(protocol))
            .map(String::as_str)
            .filter(|a| !a.is_empty())
    }

    pub fn get_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Vec<S3ObjectMetadata> {
        let mut sso = self
            .sso_ranges
            .range((stream_id, 0, 0)..=(stream_id, u64::MAX, u64::MAX))
            .take_while(|&(&(_, range_start, _), _)| range_start < end_offset)
            .filter(|&(_, &range_end)| range_end > start_offset)
            .map(|(&(_, range_start, object_id), _)| (range_start, object_id))
            .peekable();
        let mut stream_objs = self
            .stream_objects_in(stream_id, start_offset, end_offset)
            .peekable();

        let mut result = Vec::new();
        while result.len() < limit {
            match (
                sso.peek().copied(),
                stream_objs.peek().map(|&(&key, _)| key),
            ) {
                (Some((sso_start, object_id)), stream_next) => {
                    if !stream_next.is_some_and(|(_, so_start, _)| so_start < sso_start) {
                        result.push(self.stream_set_objects[&object_id].object.clone());
                        sso.next();
                    } else {
                        result.push(stream_objs.next().unwrap().1.object.clone());
                    }
                }
                (None, Some(_)) => result.push(stream_objs.next().unwrap().1.object.clone()),
                (None, None) => break,
            }
        }

        result
    }

    fn stream_objects_in(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
    ) -> impl Iterator<Item = (&StreamOffsetKey, &StreamObjectRow)> {
        self.stream_objects
            .range((stream_id, 0, 0)..=(stream_id, u64::MAX, u64::MAX))
            .take_while(move |&(&(_, range_start, _), _)| range_start < end_offset)
            .filter(move |(_, row)| row.object.offset_ranges[0].end_offset > start_offset)
    }

    pub fn get_stream_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Vec<S3ObjectMetadata> {
        self.stream_objects_in(stream_id, start_offset, end_offset)
            .take(limit)
            .map(|(_, row)| row.object.clone())
            .collect()
    }

    pub fn get_server_objects(&self, node_id: i32) -> Vec<S3ObjectMetadata> {
        self.sso_by_node
            .range((node_id, 0)..=(node_id, u64::MAX))
            .map(|(&(_, object_id), ())| self.stream_set_objects[&object_id].object.clone())
            .collect()
    }

    pub fn is_object_exist(&self, object_id: u64) -> bool {
        self.stream_set_objects.contains_key(&object_id)
            || self.prepared.contains_key(&object_id)
            || self.stream_object_ids.contains_key(&object_id)
    }

    pub fn objects_count(&self) -> usize {
        self.stream_set_objects.len() + self.stream_objects.len()
    }

    pub fn prepared_objects_count(&self) -> usize {
        self.prepared.len()
    }

    pub fn peek_destroyed_objects(&self, limit: usize) -> Vec<(u64, CompactOperations)> {
        self.mark_destroyed.values().take(limit).copied().collect()
    }

    pub fn stream_object_keys_page(
        &self,
        start_after: Option<StreamOffsetKey>,
        limit: usize,
    ) -> Vec<StreamOffsetKey> {
        let from = match start_after {
            Some(key) => Bound::Excluded(key),
            None => Bound::Unbounded,
        };

        self.stream_objects
            .range((from, Bound::Unbounded))
            .take(limit)
            .map(|(key, _)| *key)
            .collect()
    }

    pub fn get_kv(&self, key: &str) -> Option<Bytes> {
        self.kv.get(key).cloned()
    }

    pub fn list_kv(&self, prefix: &str) -> Vec<(String, Bytes)> {
        self.list_kv_page(prefix, None, usize::MAX)
    }

    pub fn list_kv_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Vec<(String, Bytes)> {
        let from = match start_after {
            Some(after) if after >= prefix => Bound::Excluded(after.to_owned()),
            _ => Bound::Included(prefix.to_owned()),
        };

        self.kv
            .range((from, Bound::Unbounded))
            .take_while(|(k, _)| k.starts_with(prefix))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use s3stream::{
        CommitStreamSetObjectRequest, CompactStreamObjectRequest, ObjectStreamRange, S3ObjectType,
        StreamState,
    };

    use crate::apply::apply;
    use crate::command::{Command, Outcome};
    use crate::state::State;

    const NODE_1: i32 = 1;
    const NODE_2: i32 = 2;
    const EPOCH_1: i64 = 10;
    const EPOCH_2: i64 = 20;

    fn populated() -> (State, u64) {
        let mut state = State::new();
        for (node_id, node_epoch, addr, kafka) in [
            (
                NODE_1,
                EPOCH_1,
                "http://n1:9090",
                Some("n1:9092".to_owned()),
            ),
            (NODE_2, EPOCH_2, "", None),
        ] {
            apply(
                &mut state,
                &Command::RegisterNode {
                    node_id,
                    node_epoch,
                    http_address: addr.into(),
                    slots: 1,
                    protocol_addresses: kafka
                        .map(|address: String| BTreeMap::from([("kafka".to_owned(), address)]))
                        .unwrap_or_default(),
                },
            )
            .unwrap();
        }
        let Outcome::Id(stream_id) = apply(
            &mut state,
            &Command::CreateStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
            },
        )
        .unwrap() else {
            panic!("expected id");
        };
        apply(
            &mut state,
            &Command::OpenStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                stream_id,
                epoch: 1,
            },
        )
        .unwrap();
        apply(
            &mut state,
            &Command::PrepareObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                count: 4,
                ttl_ms: 60_000,
                now_ms: 0,
            },
        )
        .unwrap();

        for (object_id, start, end) in [(0, 0, 8), (2, 8, 16)] {
            apply(
                &mut state,
                &Command::CommitStreamSetObject {
                    node_id: NODE_1,
                    node_epoch: EPOCH_1,
                    request: CommitStreamSetObjectRequest {
                        object_id,
                        object_size: 64,
                        attributes: 0,
                        stream_ranges: vec![ObjectStreamRange {
                            stream_id,
                            epoch: 1,
                            start_offset: start,
                            end_offset: end,
                            size: 64,
                        }],
                        stream_objects: vec![],
                        compacted_object_ids: vec![],
                    },
                    now_ms: 1,
                },
            )
            .unwrap();
        }
        apply(
            &mut state,
            &Command::CompactStreamObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request: CompactStreamObjectRequest {
                    object_id: 1,
                    object_size: 32,
                    stream_id,
                    stream_epoch: 1,
                    start_offset: 0,
                    end_offset: 4,
                    source_object_ids: vec![],
                    operations: vec![],
                    attributes: 0,
                },
                now_ms: 2,
            },
        )
        .unwrap();
        (state, stream_id)
    }

    #[test]
    fn stream_lookups() {
        let (state, stream_id) = populated();
        let meta = state.get_stream(stream_id).unwrap();
        assert_eq!(meta.end_offset, 16);
        assert_eq!(meta.state, StreamState::Opened);
        assert!(state.get_stream(999).is_none());
        assert_eq!(state.get_streams(&[stream_id, 999]).len(), 1);
        assert_eq!(state.get_node_address(NODE_1), Some("http://n1:9090"));
        assert_eq!(
            state.get_node_address(NODE_2),
            None,
            "empty address is unadvertised"
        );
        assert_eq!(
            state.get_node_protocol_address(NODE_1, "kafka"),
            Some("n1:9092")
        );
        assert_eq!(state.get_node_protocol_address(NODE_2, "kafka"), None);
        assert_eq!(state.get_node_protocol_address(999, "kafka"), None);
    }

    #[test]
    fn get_opening_streams_filters_by_node() {
        let (state, stream_id) = populated();
        let opening = state.get_opening_streams(NODE_1);
        assert_eq!(opening.len(), 1);
        assert_eq!(opening[0].stream_id, stream_id);
        assert!(state.get_opening_streams(NODE_2).is_empty());
    }

    #[test]
    fn get_objects_merges_sorts_and_limits() {
        let (state, stream_id) = populated();

        let all = state.get_objects(stream_id, 0, u64::MAX, 100);
        assert_eq!(
            all.iter()
                .map(|o| (o.object_id, o.object_type))
                .collect::<Vec<_>>(),
            vec![
                (0, S3ObjectType::StreamSet),
                (1, S3ObjectType::Stream),
                (2, S3ObjectType::StreamSet)
            ]
        );

        let mid = state.get_objects(stream_id, 4, 6, 100);
        assert_eq!(mid.iter().map(|o| o.object_id).collect::<Vec<_>>(), vec![0]);

        let limited = state.get_objects(stream_id, 0, u64::MAX, 2);
        assert_eq!(
            limited.iter().map(|o| o.object_id).collect::<Vec<_>>(),
            vec![0, 1]
        );

        assert!(state.get_objects(999, 0, u64::MAX, 100).is_empty());
    }

    #[test]
    fn get_stream_objects_returns_only_stream_objects() {
        let (state, stream_id) = populated();
        let objects = state.get_stream_objects(stream_id, 0, u64::MAX, 100);
        assert_eq!(
            objects.iter().map(|o| o.object_id).collect::<Vec<_>>(),
            vec![1]
        );
        assert!(
            state
                .get_stream_objects(stream_id, 4, u64::MAX, 100)
                .is_empty()
        );
    }

    #[test]
    fn get_server_objects_filters_by_node() {
        let (state, _) = populated();
        let objects = state.get_server_objects(NODE_1);
        assert_eq!(
            objects.iter().map(|o| o.object_id).collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert!(state.get_server_objects(NODE_2).is_empty());
    }

    #[test]
    fn object_existence_and_count() {
        let (state, _) = populated();
        assert!(state.is_object_exist(0), "committed stream-set object");
        assert!(state.is_object_exist(1), "committed stream object");
        assert!(state.is_object_exist(3), "still under prepare lease");
        assert!(!state.is_object_exist(99));
        assert_eq!(state.objects_count(), 3);
    }

    #[test]
    fn kv_get_and_prefix_list() {
        let mut state = State::new();
        for (key, value) in [("a", "1"), ("ab", "2"), ("b", "3")] {
            apply(
                &mut state,
                &Command::PutKv {
                    key: key.into(),
                    value: Bytes::from(value),
                },
            )
            .unwrap();
        }
        assert_eq!(state.get_kv("ab"), Some(Bytes::from("2")));
        assert_eq!(state.get_kv("missing"), None);
        let listed: Vec<String> = state.list_kv("a").into_iter().map(|(k, _)| k).collect();
        assert_eq!(listed, vec!["a", "ab"]);
        assert_eq!(state.list_kv("").len(), 3);
        assert!(state.list_kv("z").is_empty());
    }

    #[test]
    fn stream_object_keys_page_walks_the_catalog() {
        let (state, stream_id) = populated();
        let all = state.stream_object_keys_page(None, 100);
        assert_eq!(all, vec![(stream_id, 0, 1)]);
        assert!(
            state
                .stream_object_keys_page(Some((stream_id, 0, 1)), 100)
                .is_empty()
        );
        assert!(state.stream_object_keys_page(None, 0).is_empty());
    }
}
