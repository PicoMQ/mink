use bytes::Bytes;
use mink_core::{
    Assembly, AssemblyHash, BuiltFile, DaySketches, File, FileVersion, IndexedFile, OffsetRange,
    Offsets, Partition, Stats, TimePartition,
};
use serde_json::Value;
use sqlx::postgres::PgArguments;
use sqlx::query::QueryAs;
use sqlx::types::Json;
use sqlx::{FromRow, PgConnection, PgPool, Postgres};

use crate::Error;
use crate::leases::{Lease, check};
use crate::ranges::{lock, set};
use crate::sketches;
use crate::stats::{FIRST_TIMESTAMP_MS, parse_stats, stats_json};

macro_rules! file_select {
    () => {
        "SELECT partition, start_offset, end_offset, spec_id, bucket, schema_id, assembly, \
         version, stats FROM file \
         WHERE topic = $1 AND partition = $2 AND covers($3, $4, start_offset, end_offset)"
    };
}

macro_rules! file_order {
    () => {
        " ORDER BY start_offset, end_offset, spec_id, bucket, schema_id, assembly"
    };
}

macro_rules! identity {
    () => {
        "topic = $1 AND partition = $2 AND start_offset = $3 AND end_offset = $4 \
         AND spec_id = $5 AND bucket IS NOT DISTINCT FROM $6 AND schema_id = $7 \
         AND assembly IS NOT DISTINCT FROM $8 AND version = $9"
    };
}

pub struct Files<'a> {
    pool: &'a PgPool,
}

#[derive(FromRow)]
struct PartsRow {
    parts: Option<Json<Vec<(i64, i64)>>>,
}

pub struct StoredFile {
    indexed: IndexedFile,
    parts: Option<Vec<OffsetRange>>,
    added_snapshot_id: Option<i64>,
    replaced_snapshot_id: Option<i64>,
}

#[derive(FromRow)]
struct StoredRow {
    #[sqlx(flatten)]
    file: FileRow,
    parts: Option<Json<Vec<(i64, i64)>>>,
    added_snapshot_id: Option<i64>,
    replaced_snapshot_id: Option<i64>,
}

#[derive(FromRow)]
struct FileRow {
    partition: i32,
    start_offset: i64,
    end_offset: i64,
    spec_id: i32,
    bucket: Option<i32>,
    schema_id: i32,
    assembly: Option<String>,
    version: i16,
    stats: Json<Value>,
}

impl<'a> Files<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> Files<'a> {
        Files { pool }
    }

    pub async fn add(
        &self,
        lease: &Lease,
        files: &[BuiltFile],
        sketches: &DaySketches,
    ) -> Result<Offsets, Error> {
        let partition = lease.require_partition()?;
        let mut files = files.to_vec();
        files.sort_by_key(|built| {
            let file = built.indexed().file();

            (file.start_offset(), file.end_offset())
        });
        check_files(partition, &files)?;

        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;
        let range = lock(&mut transaction, partition).await?;
        let (Some(first), Some(last)) = (files.first(), files.last()) else {
            return Ok(range);
        };
        let (first, last) = (first.indexed().file(), last.indexed().file());

        let (start_offset, end_offset) = extended(&range, first, last)?;
        overlaps(&mut transaction, partition, first, last).await?;

        for built in &files {
            insert(&mut transaction, built).await?;
        }
        sketches::union(&mut transaction, partition.topic(), sketches).await?;

        set(&mut transaction, partition, start_offset, end_offset).await?;
        let range = lock(&mut transaction, partition).await?;

        transaction.commit().await?;
        Ok(range)
    }

    pub async fn stats(&self, file: &File) -> Result<Option<Stats>, Error> {
        let sql = concat!("SELECT stats FROM file WHERE ", identity!());
        let stats: Option<(Json<Value>,)> = identified(sqlx::query_as(sql), file)?
            .fetch_optional(self.pool)
            .await?;

        stats.map(|(Json(stats),)| parse_stats(&stats)).transpose()
    }

    pub async fn footers(&self, files: &[File]) -> Result<Vec<Bytes>, Error> {
        let sql = concat!("SELECT footer FROM file WHERE ", identity!());
        let mut footers = Vec::with_capacity(files.len());

        for file in files {
            let footer: Option<(Vec<u8>,)> = identified(sqlx::query_as(sql), file)?
                .fetch_optional(self.pool)
                .await?;
            let (bytes,) = footer.ok_or_else(|| Error::MissingFooter(file.clone()))?;

            footers.push(Bytes::from(bytes));
        }

        Ok(footers)
    }

    pub async fn parts(&self, file: &File) -> Result<Option<Vec<OffsetRange>>, Error> {
        let mut connection = self.pool.acquire().await?;

        stored_parts(&mut connection, file).await
    }

    pub async fn page(
        &self,
        topic: &str,
        partition: Option<i32>,
        bucket: Option<i32>,
        after: Option<&File>,
        limit: i64,
    ) -> Result<Vec<StoredFile>, Error> {
        let rows: Vec<StoredRow> = sqlx::query_as(
            "SELECT partition, start_offset, end_offset, spec_id, bucket, schema_id, assembly, \
             version, stats, parts, added_snapshot_id, replaced_snapshot_id FROM file \
             WHERE topic = $1 AND replaced_snapshot_id IS NULL \
             AND ($2::INTEGER IS NULL OR partition = $2) AND ($3::INTEGER IS NULL OR bucket = $3) \
             AND ($4::INTEGER IS NULL OR (partition, start_offset, end_offset, spec_id, \
                 coalesce(bucket, -2147483648), schema_id, coalesce(assembly, ''), version) \
                 > ($4, $5, $6, $7, coalesce($8, -2147483648), $9, coalesce($10, ''), $11)) \
             ORDER BY partition, start_offset, end_offset, spec_id, \
                 coalesce(bucket, -2147483648), schema_id, coalesce(assembly, ''), version \
             LIMIT $12",
        )
        .bind(topic)
        .bind(partition)
        .bind(bucket)
        .bind(after.map(|file| file.partition().index()))
        .bind(after.map(File::start_offset))
        .bind(after.map(File::end_offset))
        .bind(after.map(|file| file.time().spec_id()))
        .bind(after.and_then(|file| file.time().bucket()))
        .bind(after.map(File::schema_id))
        .bind(
            after
                .and_then(File::assembly)
                .map(|hash| format!("{:016x}", hash.value())),
        )
        .bind(after.map(|file| file.version().number() as i16))
        .bind(limit)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter().map(|row| stored(topic, row)).collect()
    }

    pub async fn find(&self, file: &File) -> Result<Option<StoredFile>, Error> {
        let sql = concat!(
            "SELECT partition, start_offset, end_offset, spec_id, bucket, schema_id, assembly, \
             version, stats, parts, added_snapshot_id, replaced_snapshot_id FROM file WHERE ",
            identity!()
        );
        let row: Option<StoredRow> = identified(sqlx::query_as(sql), file)?
            .fetch_optional(self.pool)
            .await?;

        row.map(|row| stored(file.partition().topic(), row))
            .transpose()
    }

    pub async fn list(&self, offsets: &Offsets) -> Result<Vec<IndexedFile>, Error> {
        let rows: Vec<FileRow> = sqlx::query_as(concat!(
            file_select!(),
            " AND replaced_snapshot_id IS NULL",
            file_order!()
        ))
        .bind(offsets.partition().topic())
        .bind(offsets.partition().index())
        .bind(offsets.start_offset())
        .bind(offsets.end_offset())
        .fetch_all(self.pool)
        .await?;

        from_rows(offsets.partition().topic(), rows)
    }
}

pub(crate) async fn stored_parts(
    connection: &mut PgConnection,
    file: &File,
) -> Result<Option<Vec<OffsetRange>>, Error> {
    let sql = concat!("SELECT parts FROM file WHERE ", identity!());
    let row: Option<PartsRow> = identified(sqlx::query_as(sql), file)?
        .fetch_optional(&mut *connection)
        .await?;

    let parts = row.and_then(|row| row.parts).map(|Json(parts)| {
        parts
            .into_iter()
            .map(|(start, end)| OffsetRange::new(start, end))
            .collect::<Result<Vec<_>, _>>()
    });

    Ok(parts.transpose()?)
}

pub(crate) async fn listed_in(
    pool: &PgPool,
    offsets: &Offsets,
    snapshot_id: i64,
) -> Result<Vec<IndexedFile>, Error> {
    let rows: Vec<FileRow> = sqlx::query_as(concat!(
        file_select!(),
        " AND (replaced_snapshot_id IS NULL OR replaced_snapshot_id > $5) \
         AND (added_snapshot_id IS NULL OR added_snapshot_id <= $5)",
        file_order!()
    ))
    .bind(offsets.partition().topic())
    .bind(offsets.partition().index())
    .bind(offsets.start_offset())
    .bind(offsets.end_offset())
    .bind(snapshot_id)
    .fetch_all(pool)
    .await?;

    from_rows(offsets.partition().topic(), rows)
}

pub(crate) async fn insert(connection: &mut PgConnection, built: &BuiltFile) -> Result<(), Error> {
    insert_row(connection, built, None, None).await
}

pub(crate) async fn insert_assembled(
    connection: &mut PgConnection,
    assembly: &Assembly,
    snapshot_id: i64,
) -> Result<(), Error> {
    let parts: Vec<(i64, i64)> = assembly
        .parts()
        .iter()
        .map(|part| (part.start(), part.end()))
        .collect();

    insert_row(
        connection,
        assembly.built(),
        Some(Json(parts)),
        Some(snapshot_id),
    )
    .await
}

async fn insert_row(
    connection: &mut PgConnection,
    built: &BuiltFile,
    parts: Option<Json<Vec<(i64, i64)>>>,
    added_snapshot_id: Option<i64>,
) -> Result<(), Error> {
    let indexed = built.indexed();
    let file = indexed.file();

    sqlx::query(
        "INSERT INTO file (topic, partition, start_offset, end_offset, spec_id, bucket, \
         schema_id, assembly, version, stats, footer, parts, added_snapshot_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
    )
    .bind(file.partition().topic())
    .bind(file.partition().index())
    .bind(file.start_offset())
    .bind(file.end_offset())
    .bind(file.time().spec_id())
    .bind(file.time().bucket())
    .bind(file.schema_id())
    .bind(assembly(file))
    .bind(version(file.version())?)
    .bind(Json(stats_json(indexed.stats())))
    .bind(built.footer().to_vec())
    .bind(parts)
    .bind(added_snapshot_id)
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub(crate) async fn replace(
    connection: &mut PgConnection,
    file: &File,
    snapshot_id: i64,
) -> Result<(), Error> {
    let sql = concat!(
        "UPDATE file SET replaced_snapshot_id = $10 WHERE ",
        identity!(),
        " AND replaced_snapshot_id IS NULL RETURNING 1"
    );

    let updated: Vec<(i32,)> = identified(sqlx::query_as(sql), file)?
        .bind(snapshot_id)
        .fetch_all(&mut *connection)
        .await?;

    match updated.len() {
        1 => Ok(()),
        _ => Err(Error::MixedAssembly(file.clone())),
    }
}

pub(crate) async fn remove_unlisted(
    connection: &mut PgConnection,
    topic: &str,
) -> Result<(), Error> {
    sqlx::query(
        "DELETE FROM file f WHERE f.topic = $1 AND (\
             (NOT EXISTS (SELECT 1 FROM snapshot_offsets o \
                 WHERE o.topic = f.topic AND o.partition = f.partition \
                 AND covers(o.start_offset, o.end_offset, f.start_offset, f.end_offset)) \
             AND NOT EXISTS (SELECT 1 FROM indexed_range r \
                 WHERE r.topic = f.topic AND r.partition = f.partition \
                 AND covers(r.start_offset, r.end_offset, f.start_offset, f.end_offset))) \
             OR f.replaced_snapshot_id <= (SELECT MIN(id) FROM snapshot s WHERE s.topic = f.topic))",
    )
    .bind(topic)
    .execute(&mut *connection)
    .await?;

    Ok(())
}

fn identified<'q, O>(
    query: QueryAs<'q, Postgres, O, PgArguments>,
    file: &'q File,
) -> Result<QueryAs<'q, Postgres, O, PgArguments>, Error> {
    Ok(query
        .bind(file.partition().topic())
        .bind(file.partition().index())
        .bind(file.start_offset())
        .bind(file.end_offset())
        .bind(file.time().spec_id())
        .bind(file.time().bucket())
        .bind(file.schema_id())
        .bind(assembly(file))
        .bind(version(file.version())?))
}

fn assembly(file: &File) -> Option<String> {
    file.assembly()
        .map(|assembly| format!("{:016x}", assembly.value()))
}

fn check_files(partition: &Partition, files: &[BuiltFile]) -> Result<(), Error> {
    let outside = files
        .iter()
        .map(|built| built.indexed().file())
        .find(|file| file.partition() != partition || file.assembly().is_some());
    if let Some(file) = outside {
        return Err(Error::FileOutsideLease(file.clone()));
    }

    let mismatched = files.windows(2).find(|pair| {
        let (before, after) = (pair[0].indexed().file(), pair[1].indexed().file());
        let same_range = (before.start_offset(), before.end_offset())
            == (after.start_offset(), after.end_offset());

        !same_range && before.end_offset() != after.start_offset()
    });
    match mismatched {
        Some(pair) => Err(Error::NotAdjacent(pair[1].indexed().file().clone())),
        None => Ok(()),
    }
}

fn extended(range: &Offsets, first: &File, last: &File) -> Result<(i64, i64), Error> {
    if first.start_offset() == range.end_offset() {
        Ok((range.start_offset(), last.end_offset()))
    } else if last.end_offset() == range.start_offset() {
        Ok((first.start_offset(), range.end_offset()))
    } else {
        Err(Error::NotAdjacent(first.clone()))
    }
}

async fn overlaps(
    connection: &mut PgConnection,
    partition: &Partition,
    first: &File,
    last: &File,
) -> Result<(), Error> {
    let overlapping: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM file WHERE topic = $1 AND partition = $2 AND assembly IS NULL \
         AND start_offset < $4 AND $3 < end_offset LIMIT 1",
    )
    .bind(partition.topic())
    .bind(partition.index())
    .bind(first.start_offset())
    .bind(last.end_offset())
    .fetch_optional(&mut *connection)
    .await?;

    match overlapping {
        Some(_) => Err(Error::OverlappingFile(first.clone())),
        None => Ok(()),
    }
}

pub(crate) async fn first_timestamp_ms(
    connection: &mut PgConnection,
    topic: &str,
) -> Result<Option<i64>, Error> {
    let first: Option<i64> = sqlx::query_scalar(
        "SELECT MIN((f.stats ->> $2)::BIGINT) FROM file f JOIN indexed_range r \
         ON r.topic = f.topic AND r.partition = f.partition \
         WHERE f.topic = $1 AND f.replaced_snapshot_id IS NULL \
         AND covers(r.start_offset, r.end_offset, f.start_offset, f.end_offset)",
    )
    .bind(topic)
    .bind(FIRST_TIMESTAMP_MS)
    .fetch_one(&mut *connection)
    .await?;

    Ok(first)
}

pub(crate) async fn starts_at(
    connection: &mut PgConnection,
    range: &Offsets,
    offset: i64,
) -> Result<bool, Error> {
    let found: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM file WHERE topic = $1 AND partition = $2 AND start_offset = $3 \
         AND covers($4, $5, start_offset, end_offset) LIMIT 1",
    )
    .bind(range.partition().topic())
    .bind(range.partition().index())
    .bind(offset)
    .bind(range.start_offset())
    .bind(range.end_offset())
    .fetch_optional(&mut *connection)
    .await?;

    Ok(found.is_some())
}

pub(crate) async fn inside_live_assembly(
    connection: &mut PgConnection,
    partition: &Partition,
    offset: i64,
) -> Result<bool, Error> {
    let found: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM file WHERE topic = $1 AND partition = $2 AND assembly IS NOT NULL \
         AND replaced_snapshot_id IS NULL AND start_offset < $3 AND $3 < end_offset LIMIT 1",
    )
    .bind(partition.topic())
    .bind(partition.index())
    .bind(offset)
    .fetch_optional(&mut *connection)
    .await?;

    Ok(found.is_some())
}

fn version(version: FileVersion) -> Result<i16, Error> {
    i16::try_from(version.number())
        .map_err(|_| Error::OutOfRange(format!("file version {}", version.number())))
}

impl StoredFile {
    pub fn indexed(&self) -> &IndexedFile {
        &self.indexed
    }

    pub fn parts(&self) -> Option<&[OffsetRange]> {
        self.parts.as_deref()
    }

    pub fn added_snapshot_id(&self) -> Option<i64> {
        self.added_snapshot_id
    }

    pub fn replaced_snapshot_id(&self) -> Option<i64> {
        self.replaced_snapshot_id
    }
}

fn stored(topic: &str, row: StoredRow) -> Result<StoredFile, Error> {
    let parts = row
        .parts
        .map(|Json(parts)| {
            parts
                .into_iter()
                .map(|(start, end)| OffsetRange::new(start, end))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;

    Ok(StoredFile {
        indexed: from_row(topic, row.file)?,
        parts,
        added_snapshot_id: row.added_snapshot_id,
        replaced_snapshot_id: row.replaced_snapshot_id,
    })
}

fn from_rows(topic: &str, rows: Vec<FileRow>) -> Result<Vec<IndexedFile>, Error> {
    rows.into_iter().map(|row| from_row(topic, row)).collect()
}

fn from_row(topic: &str, row: FileRow) -> Result<IndexedFile, Error> {
    let FileRow {
        partition: index,
        start_offset,
        end_offset,
        spec_id,
        bucket,
        schema_id,
        assembly,
        version: number,
        stats: Json(stats),
    } = row;

    let corrupt = |what: String| Error::Corrupt(what);
    let version = u16::try_from(number)
        .ok()
        .and_then(FileVersion::from_number)
        .ok_or_else(|| corrupt(format!("file version {number}")))?;
    let partition = Partition::new(topic, index)?;
    let time = TimePartition::new(spec_id, bucket);

    let assembly = assembly
        .map(|assembly| {
            u64::from_str_radix(&assembly, 16)
                .map(AssemblyHash::new)
                .map_err(|_| corrupt(format!("assembly {assembly}")))
        })
        .transpose()?;
    let file = File::identified(
        partition,
        OffsetRange::new(start_offset, end_offset)?,
        time,
        schema_id,
        assembly,
        version,
    )?;

    Ok(IndexedFile::new(file, parse_stats(&stats)?))
}

#[cfg(test)]
mod tests {
    use mink_core::Scope;

    use super::*;
    use crate::testing::{self, INDEXER, TTL, WRITER, file};

    #[tokio::test]
    async fn files_extend_the_indexed_range_at_either_end() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let lease = testing::partition_lease(&store, &topic, 0, INDEXER).await;
        store.indexed_ranges().start(&lease, 100).await.unwrap();

        let live = store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[file(&topic, 0, 110, 120), file(&topic, 0, 100, 110)]),
                &DaySketches::default(),
            )
            .await
            .unwrap();
        let backfilled = store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[file(&topic, 0, 50, 100)]),
                &DaySketches::default(),
            )
            .await
            .unwrap();

        assert_eq!((live.start_offset(), live.end_offset()), (100, 120));
        assert_eq!(
            (backfilled.start_offset(), backfilled.end_offset()),
            (50, 120)
        );
        let committed = file(&topic, 0, 50, 100);
        assert_eq!(
            store.files().stats(committed.file()).await.unwrap(),
            Some(committed.stats().clone())
        );
    }

    #[tokio::test]
    async fn files_must_touch_the_indexed_range_without_gaps_or_overlaps() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let lease = testing::partition_lease(&store, &topic, 0, INDEXER).await;
        store.indexed_ranges().start(&lease, 0).await.unwrap();
        store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[file(&topic, 0, 0, 10)]),
                &DaySketches::default(),
            )
            .await
            .unwrap();

        let gap = store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[file(&topic, 0, 11, 20)]),
                &DaySketches::default(),
            )
            .await;
        let inner_gap = store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[file(&topic, 0, 10, 20), file(&topic, 0, 21, 30)]),
                &DaySketches::default(),
            )
            .await;
        store.indexed_ranges().trim(&lease, 10).await.unwrap();
        let over_a_trimmed_file = store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[file(&topic, 0, 5, 10)]),
                &DaySketches::default(),
            )
            .await;

        assert!(matches!(gap, Err(Error::NotAdjacent(_))), "{gap:?}");
        assert!(
            matches!(inner_gap, Err(Error::NotAdjacent(_))),
            "{inner_gap:?}"
        );
        assert!(
            matches!(over_a_trimmed_file, Err(Error::OverlappingFile(_))),
            "{over_a_trimmed_file:?}"
        );
    }

    #[tokio::test]
    async fn a_lease_adds_only_files_of_its_partition() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 2).await;
        let lease = testing::partition_lease(&store, &topic, 0, INDEXER).await;
        store.indexed_ranges().start(&lease, 0).await.unwrap();
        let snapshots = store
            .leases()
            .acquire(&Scope::Topic(topic.name().to_owned()), WRITER, TTL)
            .await
            .unwrap()
            .unwrap();

        let other_partition = store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[file(&topic, 1, 0, 10)]),
                &DaySketches::default(),
            )
            .await;
        let wrong_scope = store
            .files()
            .add(
                &snapshots,
                &mink_conformance::built(&[file(&topic, 0, 0, 10)]),
                &DaySketches::default(),
            )
            .await;

        assert!(
            matches!(other_partition, Err(Error::FileOutsideLease(_))),
            "{other_partition:?}"
        );
        assert!(
            matches!(wrong_scope, Err(Error::LeaseScope(_))),
            "{wrong_scope:?}"
        );
    }

    #[tokio::test]
    async fn list_returns_the_stored_files_inside_a_range() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let lease = testing::partition_lease(&store, &topic, 0, INDEXER).await;
        let partition = Partition::new(topic.name(), 0).unwrap();
        store.indexed_ranges().start(&lease, 0).await.unwrap();

        store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[
                    file(&topic, 0, 0, 10),
                    file(&topic, 0, 10, 20),
                    file(&topic, 0, 20, 30),
                ]),
                &DaySketches::default(),
            )
            .await
            .unwrap();
        let inside = store
            .files()
            .list(&Offsets::new(partition.clone(), 10, 30).unwrap())
            .await
            .unwrap();
        let straddling = store
            .files()
            .list(&Offsets::new(partition.clone(), 5, 25).unwrap())
            .await
            .unwrap();

        let ranges = |files: Vec<IndexedFile>| -> Vec<_> {
            files
                .into_iter()
                .map(|indexed| (indexed.file().start_offset(), indexed.file().end_offset()))
                .collect()
        };
        assert_eq!(ranges(inside), [(10, 20), (20, 30)]);
        assert_eq!(ranges(straddling), [(10, 20)]);
    }

    #[tokio::test]
    async fn assembled_files_replace_their_parts_from_their_snapshot_on() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let indexer = &testing::started(&store, &topic).await[0];
        let writer = testing::topic_lease(&store, &topic, WRITER).await;
        let indexed = [
            file(&topic, 0, 0, 10),
            file(&topic, 0, 10, 20),
            file(&topic, 0, 20, 30),
        ];
        store
            .files()
            .add(
                indexer,
                &mink_conformance::built(&indexed),
                &DaySketches::default(),
            )
            .await
            .unwrap();
        let first = testing::snapshot(&writer, &topic, 1, &[(0, 30)]);
        store
            .snapshots()
            .commit(&writer, &first, &[], &[], &[])
            .await
            .unwrap();

        let replaced = vec![indexed[0].file().clone(), indexed[1].file().clone()];
        let parts = mink_conformance::ranges(&[(0, 10), (10, 20)]);
        let assembled = File::assembled(
            indexed[0].file().partition().clone(),
            indexed[0].file().time(),
            0,
            FileVersion::V1,
            &parts,
        )
        .unwrap();
        let built = |file: &File| {
            let indexed = IndexedFile::new(file.clone(), Stats::new(20, 2000, 0));

            BuiltFile::new(indexed, Bytes::from_static(b"footer"))
        };
        let assembly = Assembly::new(built(&assembled), parts.clone(), replaced.clone()).unwrap();
        let wrong_parts = mink_conformance::ranges(&[(0, 5), (5, 20)]);
        let misassembled = File::assembled(
            indexed[0].file().partition().clone(),
            indexed[0].file().time(),
            0,
            FileVersion::V1,
            &wrong_parts,
        )
        .unwrap();
        let mismatch = Assembly::new(built(&misassembled), wrong_parts, replaced.clone()).unwrap();
        let second = testing::snapshot(&writer, &topic, 2, &[(0, 30)]);

        let rejected = store
            .snapshots()
            .commit(&writer, &second, &[], &[], &[mismatch])
            .await;
        assert!(
            matches!(rejected, Err(Error::MixedAssembly(_))),
            "{rejected:?}"
        );
        store
            .snapshots()
            .commit(&writer, &second, &[], &[], std::slice::from_ref(&assembly))
            .await
            .unwrap();

        let paths = |files: Vec<IndexedFile>| -> Vec<(i64, i64, bool)> {
            files
                .iter()
                .map(|indexed| {
                    let file = indexed.file();

                    (
                        file.start_offset(),
                        file.end_offset(),
                        file.assembly().is_some(),
                    )
                })
                .collect()
        };
        let snapshots = store.snapshots();
        assert_eq!(
            paths(snapshots.files(&first).await.unwrap()),
            [(0, 10, false), (10, 20, false), (20, 30, false)]
        );
        assert_eq!(
            paths(snapshots.files(&second).await.unwrap()),
            [(0, 20, true), (20, 30, false)]
        );
        assert_eq!(store.files().parts(&assembled).await.unwrap(), Some(parts));
        assert_eq!(
            store
                .files()
                .footers(std::slice::from_ref(&assembled))
                .await
                .unwrap(),
            [Bytes::from_static(b"footer")]
        );
        let unstored = file(&topic, 0, 40, 50).file().clone();
        assert!(matches!(
            store.files().footers(&[unstored]).await,
            Err(Error::MissingFooter(_))
        ));

        let third = testing::snapshot(&writer, &topic, 3, &[(0, 30)]);
        let again = snapshots
            .commit(&writer, &third, &[], &[], &[assembly])
            .await;
        assert!(matches!(again, Err(Error::MixedAssembly(_))), "{again:?}");

        snapshots.expire(&writer, 1).await.unwrap();
        assert_eq!(store.files().stats(&replaced[0]).await.unwrap(), None);
        assert!(store.files().stats(&assembled).await.unwrap().is_some());
    }
}
