use std::time::Duration;

use mink_core::{Object, ObjectKind, Snapshot, millis};
use sqlx::{PgConnection, PgPool};

use crate::Error;
use crate::leases::{Lease, check};

pub struct Objects<'a> {
    pool: &'a PgPool,
}

impl<'a> Objects<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> Objects<'a> {
        Objects { pool }
    }

    pub async fn collectable(&self, topic: &str, grace: Duration) -> Result<Vec<Object>, Error> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT path, kind FROM object WHERE topic = $1 \
             AND unreferenced_at_ms IS NOT NULL AND unreferenced_at_ms <= now_ms() - $2 \
             ORDER BY path",
        )
        .bind(topic)
        .bind(millis(grace))
        .fetch_all(self.pool)
        .await?;

        rows.into_iter().map(object).collect()
    }

    pub async fn list(&self, topic: &str) -> Result<Vec<Object>, Error> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT path, kind FROM object WHERE topic = $1 ORDER BY path")
                .bind(topic)
                .fetch_all(self.pool)
                .await?;

        rows.into_iter().map(object).collect()
    }

    pub async fn statistics(&self, topic: &str) -> Result<Vec<String>, Error> {
        let paths = sqlx::query_scalar(
            "SELECT path FROM object WHERE topic = $1 AND kind = 'statistics' \
             AND unreferenced_at_ms IS NULL ORDER BY path",
        )
        .bind(topic)
        .fetch_all(self.pool)
        .await?;

        Ok(paths)
    }

    pub async fn add_unreferenced(&self, topic: &str, objects: &[Object]) -> Result<(), Error> {
        let mut transaction = self.pool.begin().await?;

        for object in objects {
            sqlx::query(
                "INSERT INTO object (topic, path, kind, unreferenced_at_ms) \
                 VALUES ($1, $2, $3, now_ms()) ON CONFLICT (topic, path) DO NOTHING",
            )
            .bind(topic)
            .bind(object.path())
            .bind(object.kind().name())
            .execute(&mut *transaction)
            .await?;
        }

        transaction.commit().await?;
        Ok(())
    }

    pub async fn remove(&self, lease: &Lease, objects: &[Object]) -> Result<(), Error> {
        let topic = lease.require_topic()?;
        let paths: Vec<&str> = objects.iter().map(Object::path).collect();

        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;

        sqlx::query("DELETE FROM object WHERE topic = $1 AND path = ANY($2)")
            .bind(topic)
            .bind(&paths)
            .execute(&mut *transaction)
            .await?;

        transaction.commit().await?;
        Ok(())
    }
}

pub(crate) async fn record_commit(
    connection: &mut PgConnection,
    snapshot: &Snapshot,
    latest_id: Option<i64>,
    written: &[Object],
    released: &[Object],
) -> Result<(), Error> {
    let released: Vec<&str> = released.iter().map(Object::path).collect();

    if let Some(latest_id) = latest_id {
        sqlx::query(
            "UPDATE object SET last_snapshot_id = $2 \
             WHERE topic = $1 AND last_snapshot_id IS NULL AND path = ANY($3)",
        )
        .bind(snapshot.topic())
        .bind(latest_id)
        .bind(&released)
        .execute(&mut *connection)
        .await?;
    }

    for object in written {
        sqlx::query("INSERT INTO object (topic, path, kind) VALUES ($1, $2, $3)")
            .bind(snapshot.topic())
            .bind(object.path())
            .bind(object.kind().name())
            .execute(&mut *connection)
            .await?;
    }

    Ok(())
}

pub(crate) async fn unreference_through(
    connection: &mut PgConnection,
    topic: &str,
    through_id: i64,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE object SET unreferenced_at_ms = now_ms() WHERE topic = $1 \
         AND unreferenced_at_ms IS NULL AND last_snapshot_id IS NOT NULL AND last_snapshot_id <= $2",
    )
    .bind(topic)
    .bind(through_id)
    .execute(&mut *connection)
    .await?;

    Ok(())
}

fn object((path, kind): (String, String)) -> Result<Object, Error> {
    let kind = ObjectKind::from_name(&kind)
        .ok_or_else(|| Error::Corrupt(format!("object kind {kind}")))?;

    Ok(Object::new(path, kind))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mink_core::{Object, ObjectKind, Topic};

    use crate::testing::{self, WRITER, snapshot};
    use crate::{Lease, Store};

    fn written(id: i64, manifests: usize) -> Vec<Object> {
        let mut objects = vec![
            Object::new(format!("v{id}.metadata.json"), ObjectKind::TableMetadata),
            Object::new(format!("snap-{id}.avro"), ObjectKind::ManifestList),
        ];
        objects.extend(
            (0..manifests).map(|k| Object::new(format!("{id}-m{k}.avro"), ObjectKind::Manifest)),
        );

        objects
    }

    fn paths(objects: Vec<Object>) -> Vec<String> {
        let mut paths: Vec<String> = objects
            .into_iter()
            .map(|object| object.path().to_owned())
            .collect();
        paths.sort();

        paths
    }

    async fn collectable(store: &Store, topic: &Topic) -> Vec<String> {
        paths(
            store
                .objects()
                .collectable(topic.name(), Duration::ZERO)
                .await
                .unwrap(),
        )
    }

    async fn commit(
        store: &Store,
        lease: &Lease,
        topic: &Topic,
        id: i64,
        objects: &[Object],
        released: &[Object],
    ) {
        let mut all = Vec::new();
        if id > 1 {
            all.extend(written(id - 1, 0));
        }

        all.extend_from_slice(released);

        store
            .snapshots()
            .commit(
                lease,
                &snapshot(lease, topic, id, &[(0, 0)]),
                objects,
                &all,
                &[],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_commit_records_its_objects_and_releases_what_it_replaced() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        testing::started(&store, &topic).await;
        let lease = testing::topic_lease(&store, &topic, WRITER).await;
        let released = [Object::new("1-m0.avro", ObjectKind::Manifest)];

        commit(&store, &lease, &topic, 1, &written(1, 1), &[]).await;
        commit(&store, &lease, &topic, 2, &written(2, 2), &released).await;
        commit(&store, &lease, &topic, 3, &written(3, 0), &[]).await;

        store.snapshots().expire(&lease, 1).await.unwrap();
        let first = collectable(&store, &topic).await;
        store.snapshots().expire(&lease, 2).await.unwrap();
        let second = collectable(&store, &topic).await;

        assert_eq!(
            paths(store.objects().list(topic.name()).await.unwrap()).len(),
            9
        );
        assert_eq!(first, ["1-m0.avro", "snap-1.avro", "v1.metadata.json"]);
        assert_eq!(
            second,
            [
                "1-m0.avro",
                "snap-1.avro",
                "snap-2.avro",
                "v1.metadata.json",
                "v2.metadata.json"
            ]
        );
    }

    #[tokio::test]
    async fn objects_are_collectable_only_after_the_grace_period() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        testing::started(&store, &topic).await;
        let lease = testing::topic_lease(&store, &topic, WRITER).await;
        commit(&store, &lease, &topic, 1, &written(1, 1), &[]).await;
        commit(&store, &lease, &topic, 2, &written(2, 0), &[]).await;

        let referenced = collectable(&store, &topic).await;
        store.snapshots().expire(&lease, 1).await.unwrap();
        let within = store
            .objects()
            .collectable(topic.name(), Duration::from_secs(3600))
            .await
            .unwrap();
        let after = collectable(&store, &topic).await;

        assert!(referenced.is_empty());
        assert!(within.is_empty());
        assert_eq!(after, ["snap-1.avro", "v1.metadata.json"]);
    }

    #[tokio::test]
    async fn expiry_and_collection_run_twice_change_nothing() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        testing::started(&store, &topic).await;
        let lease = testing::topic_lease(&store, &topic, WRITER).await;
        commit(&store, &lease, &topic, 1, &written(1, 1), &[]).await;
        commit(&store, &lease, &topic, 2, &written(2, 0), &[]).await;

        let state = async || {
            let snapshots = store.snapshots().list(topic.name()).await.unwrap();
            let recorded = paths(store.objects().list(topic.name()).await.unwrap());
            let collectable = store
                .objects()
                .collectable(topic.name(), Duration::ZERO)
                .await
                .unwrap();
            (snapshots, recorded, collectable)
        };

        store.snapshots().expire(&lease, 1).await.unwrap();
        let expired = state().await;
        store.snapshots().expire(&lease, 1).await.unwrap();
        let expired_twice = state().await;

        let collected = store
            .objects()
            .collectable(topic.name(), Duration::ZERO)
            .await
            .unwrap();
        store.objects().remove(&lease, &collected).await.unwrap();
        let removed = state().await;
        store.objects().remove(&lease, &collected).await.unwrap();
        let removed_twice = state().await;

        assert_eq!(expired, expired_twice);
        assert_eq!(removed, removed_twice);
        assert_eq!(removed.1, ["1-m0.avro", "snap-2.avro", "v2.metadata.json"]);
        assert!(removed.2.is_empty());
    }

    #[tokio::test]
    async fn unreferenced_objects_are_recorded_once() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let tail = written(1, 1);

        store
            .objects()
            .add_unreferenced(topic.name(), &tail)
            .await
            .unwrap();
        store
            .objects()
            .add_unreferenced(topic.name(), &tail)
            .await
            .unwrap();

        assert_eq!(
            paths(store.objects().list(topic.name()).await.unwrap()).len(),
            3
        );
        assert_eq!(collectable(&store, &topic).await.len(), 3);
    }
}
