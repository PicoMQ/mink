//! Selects and caches the row merger for a table's merge engine and target columns; includes the simple mergers.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use mink_common::sync::lock;
use mink_record::{Row, Scalar, Timestamp};
use mink_table::{DeleteBehavior, Descriptor, MergeEngine, SchemaId};
use mink_types::Kind;

use crate::Error;
use crate::aggregate::Aggregator;
use crate::partial::Updater;
use crate::schema::Version;
use crate::value::Value;

pub struct Decoded<'a> {
    pub schema_id: SchemaId,
    pub row: Row<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Merged {
    Old,
    New,
    Row(Value),
}

pub trait RowMerger: Send + Sync {
    fn merge(&self, old: &Decoded<'_>, new: &Decoded<'_>) -> Result<Merged, Error>;

    fn delete(&self, old: &Decoded<'_>) -> Result<Option<Value>, Error>;

    fn delete_behavior(&self) -> DeleteBehavior;

    fn is_default(&self) -> bool {
        false
    }
}

pub struct Merger {
    engine: Engine,
    delete_behavior: DeleteBehavior,
}

type Partials<M> = Mutex<HashMap<(SchemaId, Vec<usize>), Arc<M>>>;

enum Engine {
    Default {
        partial: Partials<Updater>,
    },
    FirstRow,
    Versioned {
        column: String,
        comparators: Mutex<HashMap<SchemaId, Arc<Versioned>>>,
    },
    Aggregation {
        full: Mutex<HashMap<SchemaId, Arc<Aggregator>>>,
        partial: Partials<Aggregator>,
    },
}

impl Merger {
    pub fn new(descriptor: &Descriptor) -> Result<Self, Error> {
        let delete_behavior = descriptor.delete_behavior();
        let engine = match &descriptor.options().merge_engine {
            None => Engine::Default {
                partial: Mutex::new(HashMap::new()),
            },
            Some(MergeEngine::FirstRow) => {
                if delete_behavior == DeleteBehavior::Allow {
                    return Err(Error::DeleteUnsupported("first_row"));
                }
                Engine::FirstRow
            }
            Some(MergeEngine::Versioned { column }) => {
                if delete_behavior == DeleteBehavior::Allow {
                    return Err(Error::DeleteUnsupported("versioned"));
                }
                Engine::Versioned {
                    column: column.clone(),
                    comparators: Mutex::new(HashMap::new()),
                }
            }
            Some(MergeEngine::Aggregation) => Engine::Aggregation {
                full: Mutex::new(HashMap::new()),
                partial: Mutex::new(HashMap::new()),
            },
        };

        Ok(Merger {
            engine,
            delete_behavior,
        })
    }

    pub fn configure(
        &self,
        targets: Option<&[usize]>,
        latest: &Arc<Version>,
    ) -> Result<Arc<dyn RowMerger>, Error> {
        match (&self.engine, targets) {
            (Engine::Default { .. }, None) => Ok(Arc::new(LastWrite {
                delete_behavior: self.delete_behavior,
            })),
            (Engine::Default { partial }, Some(targets)) => {
                let updater = cached(partial, (latest.id, targets.to_vec()), || {
                    Updater::new(Arc::clone(latest), targets)
                })?;

                Ok(Arc::new(Partial {
                    updater,
                    delete_behavior: self.delete_behavior,
                }))
            }
            (Engine::FirstRow, None) => Ok(Arc::new(FirstRow {
                delete_behavior: self.delete_behavior,
            })),
            (Engine::FirstRow, Some(_)) => Err(Error::PartialUnsupported("first_row")),
            (
                Engine::Versioned {
                    column,
                    comparators,
                },
                None,
            ) => Ok(cached(comparators, latest.id, || {
                Versioned::new(column, latest, self.delete_behavior)
            })?),
            (Engine::Versioned { .. }, Some(_)) => Err(Error::PartialUnsupported("versioned")),
            (Engine::Aggregation { full, .. }, None) => Ok(cached(full, latest.id, || {
                Ok(Aggregator::new(Arc::clone(latest), self.delete_behavior))
            })?),
            (Engine::Aggregation { partial, .. }, Some(targets)) => {
                Ok(cached(partial, (latest.id, targets.to_vec()), || {
                    Aggregator::partial(Arc::clone(latest), targets, self.delete_behavior)
                })?)
            }
        }
    }
}

fn cached<K: Hash + Eq, V>(
    cache: &Mutex<HashMap<K, Arc<V>>>,
    key: K,
    make: impl FnOnce() -> Result<V, Error>,
) -> Result<Arc<V>, Error> {
    let mut cache = lock(cache);
    if let Some(value) = cache.get(&key) {
        return Ok(Arc::clone(value));
    }

    let value = Arc::new(make()?);
    cache.insert(key, Arc::clone(&value));

    Ok(value)
}

pub(crate) fn field<'a>(row: &Row<'a>, i: usize) -> Option<Scalar<'a>> {
    row.get(i).copied().flatten()
}

struct LastWrite {
    delete_behavior: DeleteBehavior,
}

impl RowMerger for LastWrite {
    fn merge(&self, _old: &Decoded<'_>, _new: &Decoded<'_>) -> Result<Merged, Error> {
        Ok(Merged::New)
    }

    fn delete(&self, _old: &Decoded<'_>) -> Result<Option<Value>, Error> {
        Ok(None)
    }

    fn delete_behavior(&self) -> DeleteBehavior {
        self.delete_behavior
    }

    fn is_default(&self) -> bool {
        true
    }
}

struct Partial {
    updater: Arc<Updater>,
    delete_behavior: DeleteBehavior,
}

impl RowMerger for Partial {
    fn merge(&self, old: &Decoded<'_>, new: &Decoded<'_>) -> Result<Merged, Error> {
        self.updater.update(old, new)
    }

    fn delete(&self, old: &Decoded<'_>) -> Result<Option<Value>, Error> {
        self.updater.delete(old)
    }

    fn delete_behavior(&self) -> DeleteBehavior {
        self.delete_behavior
    }
}

struct FirstRow {
    delete_behavior: DeleteBehavior,
}

impl RowMerger for FirstRow {
    fn merge(&self, _old: &Decoded<'_>, _new: &Decoded<'_>) -> Result<Merged, Error> {
        Ok(Merged::Old)
    }

    fn delete(&self, _old: &Decoded<'_>) -> Result<Option<Value>, Error> {
        Err(Error::DeleteUnsupported("first_row"))
    }

    fn delete_behavior(&self) -> DeleteBehavior {
        self.delete_behavior
    }
}

struct Versioned {
    column: usize,
    delete_behavior: DeleteBehavior,
}

impl Versioned {
    fn new(column: &str, schema: &Version, delete_behavior: DeleteBehavior) -> Result<Self, Error> {
        let fields = schema.schema.fields();
        let index = fields
            .index_of(column)
            .ok_or_else(|| Error::VersionColumn(column.to_owned()))?;
        let data_type = fields[index].data_type();
        match data_type.kind() {
            Kind::Int | Kind::BigInt | Kind::Timestamp(_) | Kind::TimestampLtz(_) => {}
            _ => {
                return Err(Error::VersionType {
                    column: column.to_owned(),
                    data_type: data_type.clone(),
                });
            }
        }

        Ok(Versioned {
            column: index,
            delete_behavior,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum VersionKey {
    Null,
    Int(i64),
    Time(i64, u32),
}

impl VersionKey {
    fn of(scalar: Option<Scalar<'_>>) -> Self {
        match scalar {
            None => VersionKey::Null,
            Some(Scalar::Int(v)) => VersionKey::Int(i64::from(v)),
            Some(Scalar::BigInt(v)) => VersionKey::Int(v),
            Some(Scalar::Timestamp {
                at: Timestamp { millis, nanos },
                ..
            }) => VersionKey::Time(millis, nanos),
            Some(other) => unreachable!("version column type was validated, got {other:?}"),
        }
    }
}

impl RowMerger for Versioned {
    fn merge(&self, old: &Decoded<'_>, new: &Decoded<'_>) -> Result<Merged, Error> {
        let old_version = VersionKey::of(old.row.get(self.column).copied().flatten());
        let new_version = VersionKey::of(new.row.get(self.column).copied().flatten());
        Ok(if old_version <= new_version {
            Merged::New
        } else {
            Merged::Old
        })
    }

    fn delete(&self, _old: &Decoded<'_>) -> Result<Option<Value>, Error> {
        Err(Error::DeleteUnsupported("versioned"))
    }

    fn delete_behavior(&self) -> DeleteBehavior {
        self.delete_behavior
    }
}
