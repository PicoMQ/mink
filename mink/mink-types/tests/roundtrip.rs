//! Property tests that every data type survives a round trip through text, JSON and Arrow.

use arrow_schema::DataType as ArrowType;
use mink_types::{DataType, Decimal, Field, FieldId, Fields, Kind, Length, Precision};
use proptest::prelude::*;

fn length() -> impl Strategy<Value = Length> {
    (1u32..=64).prop_map(|n| Length::new(n).unwrap())
}

fn precision() -> impl Strategy<Value = Precision> {
    (0u8..=9).prop_map(|p| Precision::new(p).unwrap())
}

fn decimal() -> impl Strategy<Value = Decimal> {
    (1u8..=38)
        .prop_flat_map(|p| (Just(p), 0..=p))
        .prop_map(|(p, s)| Decimal::new(p, s).unwrap())
}

fn leaf() -> impl Strategy<Value = DataType> {
    prop_oneof![
        Just(DataType::boolean()),
        Just(DataType::tiny_int()),
        Just(DataType::small_int()),
        Just(DataType::int()),
        Just(DataType::big_int()),
        Just(DataType::float()),
        Just(DataType::double()),
        length().prop_map(DataType::char),
        Just(DataType::string()),
        length().prop_map(DataType::binary),
        Just(DataType::bytes()),
        decimal().prop_map(DataType::decimal),
        Just(DataType::date()),
        precision().prop_map(DataType::time),
        precision().prop_map(DataType::timestamp),
        precision().prop_map(DataType::timestamp_ltz),
    ]
}

fn field_name() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-z][a-z0-9_]{0,8}",
        Just("date".to_owned()),
        Just("with space".to_owned()),
        Just("back`tick".to_owned()),
        Just("quo'te".to_owned()),
        Just("ünïcødé".to_owned()),
    ]
}

type FieldParts = (String, DataType, Option<String>, Option<u32>);

fn field_parts(data_type: impl Strategy<Value = DataType>) -> impl Strategy<Value = FieldParts> {
    (
        field_name(),
        data_type,
        proptest::option::of("[ -~]{0,12}"),
        proptest::option::of(0u32..1000),
    )
}

fn fields(data_type: impl Strategy<Value = DataType> + Clone) -> impl Strategy<Value = Fields> {
    proptest::collection::vec(field_parts(data_type), 0..4).prop_map(|parts| {
        let mut names: Vec<String> = Vec::with_capacity(parts.len());
        let fields = parts
            .into_iter()
            .enumerate()
            .map(|(i, (name, data_type, description, id))| {
                let name = if names.contains(&name) {
                    format!("{name}_{i}")
                } else {
                    name
                };
                names.push(name.clone());
                let mut field = Field::new(name, data_type).unwrap();
                if let Some(description) = description {
                    field = field.with_description(description);
                }
                if let Some(id) = id {
                    field = field.with_id(FieldId(id));
                }
                field
            })
            .collect();
        Fields::new(fields).unwrap()
    })
}

fn data_type() -> impl Strategy<Value = DataType> {
    leaf()
        .prop_recursive(3, 24, 4, |inner| {
            prop_oneof![
                inner.clone().prop_map(DataType::array),
                (inner.clone(), inner.clone()).prop_map(|(k, v)| DataType::map(k, v)),
                fields(inner).prop_map(DataType::row),
            ]
        })
        .prop_flat_map(|t| (Just(t), any::<bool>()))
        .prop_map(|(t, nullable)| t.with_nullable(nullable))
}

fn without_ids(data_type: &DataType) -> DataType {
    let kind = match data_type.kind() {
        Kind::Array(e) => Kind::Array(Box::new(without_ids(e))),
        Kind::Map { key, value } => Kind::Map {
            key: Box::new(without_ids(key)),
            value: Box::new(without_ids(value)),
        },
        Kind::Row(fields) => Kind::Row(
            Fields::new(
                fields
                    .iter()
                    .map(|f| {
                        let field = Field::new(f.name(), without_ids(f.data_type())).unwrap();
                        match f.description() {
                            Some(d) => field.with_description(d),
                            None => field,
                        }
                    })
                    .collect(),
            )
            .unwrap(),
        ),
        leaf => leaf.clone(),
    };
    DataType::new(kind, data_type.is_nullable())
}

fn arrow_canonical(data_type: &DataType) -> DataType {
    let snap = |p: &Precision| match p.get() {
        0 => Precision::SECONDS,
        1..=3 => Precision::MILLIS,
        4..=6 => Precision::MICROS,
        _ => Precision::NANOS,
    };
    let kind = match data_type.kind() {
        Kind::Char(_) => Kind::String,
        Kind::Time(p) => Kind::Time(snap(p)),
        Kind::Timestamp(p) => Kind::Timestamp(snap(p)),
        Kind::TimestampLtz(p) => Kind::TimestampLtz(snap(p)),
        Kind::Array(e) => Kind::Array(Box::new(arrow_canonical(e))),
        Kind::Map { key, value } => Kind::Map {
            key: Box::new(arrow_canonical(key)),
            value: Box::new(arrow_canonical(value)),
        },
        Kind::Row(fields) => Kind::Row(
            Fields::new(
                fields
                    .iter()
                    .map(|f| {
                        let field = Field::new(f.name(), arrow_canonical(f.data_type())).unwrap();
                        match f.id() {
                            Some(id) => field.with_id(id),
                            None => field,
                        }
                    })
                    .collect(),
            )
            .unwrap(),
        ),
        leaf => leaf.clone(),
    };
    DataType::new(kind, data_type.is_nullable())
}

proptest! {
    #[test]
    fn text_round_trips(data_type in data_type()) {
        let text = data_type.to_string();
        let parsed: DataType = text.parse().unwrap_or_else(|e| panic!("{text}: {e}"));
        prop_assert_eq!(parsed, without_ids(&data_type));
    }

    #[test]
    fn json_round_trips(data_type in data_type()) {
        let json = serde_json::to_string(&data_type).unwrap();
        let parsed: DataType = serde_json::from_str(&json).unwrap_or_else(|e| panic!("{json}: {e}"));
        prop_assert_eq!(parsed, data_type);
    }

    #[test]
    fn arrow_round_trips_up_to_its_own_precision(data_type in data_type()) {
        let arrow = ArrowType::from(&data_type);
        let back = DataType::try_from(&arrow).unwrap().with_nullable(data_type.is_nullable());
        prop_assert_eq!(back, arrow_canonical(&data_type));
    }

    #[test]
    fn field_ids_are_dense_and_depth_first(data_type in data_type()) {
        let mut next = 0;
        let assigned = data_type.assign_field_ids(&mut next);
        let mut seen = Vec::new();
        collect_ids(&assigned, &mut seen);
        prop_assert_eq!(seen.len() as u32, next);
        prop_assert_eq!(seen, (0..next).collect::<Vec<_>>());
    }
}

fn collect_ids(data_type: &DataType, into: &mut Vec<u32>) {
    match data_type.kind() {
        Kind::Row(fields) => {
            for field in fields {
                into.push(field.id().expect("assigned").0);
                collect_ids(field.data_type(), into);
            }
        }
        _ => {
            for child in data_type.children() {
                collect_ids(child, into);
            }
        }
    }
}
