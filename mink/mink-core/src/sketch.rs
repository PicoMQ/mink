use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use datasketches::hash::value::raw_bytes;
use datasketches::theta::{CompactThetaSketch, ThetaSketch, ThetaSketchBuilder, ThetaUnionBuilder};

use crate::Error;

const LG_K: u8 = 12;

pub struct Sketch {
    sketch: ThetaSketch,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DaySketches {
    by_day_and_field: BTreeMap<(i64, i32), Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaySketch<'a> {
    day: i64,
    field_id: i32,
    sketch: &'a [u8],
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldSketches {
    by_field: BTreeMap<i32, Vec<u8>>,
}

impl Sketch {
    pub fn new() -> Sketch {
        let sketch = ThetaSketchBuilder::default()
            .lg_k(LG_K)
            .build()
            .expect("lg_k 12 builds a theta sketch");

        Sketch { sketch }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        if !bytes.is_empty() {
            self.sketch.update(raw_bytes::from_slice(bytes));
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        self.sketch.compact(true).serialize()
    }
}

impl Default for Sketch {
    fn default() -> Sketch {
        Sketch::new()
    }
}

impl DaySketches {
    pub fn insert(&mut self, day: i64, field_id: i32, sketch: Vec<u8>) -> Result<(), Error> {
        match self.by_day_and_field.entry((day, field_id)) {
            Entry::Vacant(vacant) => {
                vacant.insert(sketch);
            }
            Entry::Occupied(mut occupied) => {
                let unioned = union(&[occupied.get(), &sketch])?;

                occupied.insert(unioned);
            }
        }

        Ok(())
    }

    pub fn merge(&mut self, other: DaySketches) -> Result<(), Error> {
        for ((day, field_id), sketch) in other.by_day_and_field {
            self.insert(day, field_id, sketch)?;
        }

        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = DaySketch<'_>> {
        self.by_day_and_field
            .iter()
            .map(|((day, field_id), sketch)| DaySketch {
                day: *day,
                field_id: *field_id,
                sketch,
            })
    }

    pub fn by_field(&self) -> Result<FieldSketches, Error> {
        let mut days: BTreeMap<i32, Vec<&[u8]>> = BTreeMap::new();

        for sketch in self.iter() {
            days.entry(sketch.field_id).or_default().push(sketch.sketch);
        }

        let by_field = days
            .into_iter()
            .map(|(field_id, sketches)| Ok((field_id, union(&sketches)?)))
            .collect::<Result<_, Error>>()?;

        Ok(FieldSketches { by_field })
    }

    pub fn is_empty(&self) -> bool {
        self.by_day_and_field.is_empty()
    }
}

impl<'a> DaySketch<'a> {
    pub fn day(&self) -> i64 {
        self.day
    }

    pub fn field_id(&self) -> i32 {
        self.field_id
    }

    pub fn sketch(&self) -> &'a [u8] {
        self.sketch
    }
}

impl FieldSketches {
    pub fn get(&self, field_id: i32) -> Option<&[u8]> {
        self.by_field.get(&field_id).map(Vec::as_slice)
    }

    pub fn iter(&self) -> impl Iterator<Item = (i32, &[u8])> {
        self.by_field
            .iter()
            .map(|(field_id, sketch)| (*field_id, sketch.as_slice()))
    }
}

pub fn union(sketches: &[&[u8]]) -> Result<Vec<u8>, Error> {
    let mut union = ThetaUnionBuilder::default()
        .lg_k(LG_K)
        .build()
        .expect("lg_k 12 builds a theta union");

    for bytes in sketches {
        let sketch = CompactThetaSketch::deserialize(bytes)
            .map_err(|error| Error::Sketch(error.to_string()))?;

        union
            .update(&sketch)
            .map_err(|error| Error::Sketch(error.to_string()))?;
    }

    Ok(union.to_sketch(true).serialize())
}

pub fn estimate(sketch: &[u8]) -> Result<f64, Error> {
    let sketch = CompactThetaSketch::deserialize(sketch)
        .map_err(|error| Error::Sketch(error.to_string()))?;

    Ok(sketch.estimate())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use serde_json::Value;

    use super::*;

    #[test]
    fn sketches_match_datasketches() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../conformance/fixtures/statistics/sketches.json");
        let fixture: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        let cases = fixture["cases"].as_array().unwrap();

        for case in cases {
            let mut sketch = Sketch::new();
            for update in case["updates"].as_array().unwrap() {
                sketch.update(&hex::decode(update.as_str().unwrap()).unwrap());
            }

            assert_eq!(
                hex::encode(sketch.serialize()),
                case["sketch"].as_str().unwrap(),
                "{}",
                case["name"]
            );
        }
    }

    #[test]
    fn unions_count_each_value_once() {
        let (mut left, mut right) = (Sketch::new(), Sketch::new());
        for value in 0u32..300 {
            left.update(&value.to_le_bytes());
        }
        for value in 200u32..500 {
            right.update(&value.to_le_bytes());
        }
        right.update(b"");

        let mut sketches = DaySketches::default();
        sketches.insert(7, 10, left.serialize()).unwrap();
        sketches.insert(7, 10, right.serialize()).unwrap();
        sketches.insert(8, 10, Sketch::new().serialize()).unwrap();
        let first = sketches.iter().next().unwrap();
        let by_field = sketches.by_field().unwrap();

        assert_eq!((first.day(), first.field_id()), (7, 10));
        assert_eq!(estimate(first.sketch()).unwrap(), 500.0);
        assert_eq!(estimate(by_field.get(10).unwrap()).unwrap(), 500.0);
        assert!(matches!(union(&[b"no sketch"]), Err(Error::Sketch(_))));
    }
}
