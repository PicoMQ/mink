use std::collections::{BTreeMap, HashMap};

use iceberg::spec::{
    ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
};
use mink_core::Part;

use crate::binary::{DECODED_KEY, RESERVED_NAMES};
use crate::payload::PayloadSchema;
use crate::types::{PayloadField, PayloadType, path};
use crate::wire::PartSchemaRef;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldIds {
    by_path: BTreeMap<String, i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    Missing,
    NotAccepted,
    Unsupported,
    ReservedName,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Merge {
    registry_id: i32,
    based_on: i32,
    schema: Option<Schema>,
    field_ids: FieldIds,
    identity_field_ids: FieldIds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeResult {
    Merged {
        registry_id: i32,
        table_schema_id: i32,
        field_ids: FieldIds,
    },
    Rejected(Rejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSchema {
    schema_ref: PartSchemaRef,
    result: MergeResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FieldId {
    Existing(i32),
    New(usize),
}

#[derive(Debug, Clone, PartialEq)]
struct Column {
    id: FieldId,
    name: String,
    required: bool,
    kind: Kind,
}

#[derive(Debug, Clone, PartialEq)]
enum Kind {
    Primitive(PrimitiveType),
    Struct(Vec<Column>),
    List {
        element_id: FieldId,
        element_required: bool,
        element: Box<Kind>,
    },
    Map {
        key_id: FieldId,
        key: Box<Kind>,
        value_id: FieldId,
        value_required: bool,
        value: Box<Kind>,
    },
}

struct MergeState {
    renamed_suffix: String,
    by_identity_path: BTreeMap<String, FieldId>,
    by_writer_path: BTreeMap<String, FieldId>,
    new: usize,
}

struct NewFieldIds {
    next: i32,
    by_new: HashMap<usize, i32>,
}

impl FieldIds {
    pub fn get(&self, path: &str) -> Option<i32> {
        self.by_path.get(path).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, i32)> {
        self.by_path.iter().map(|(path, id)| (path.as_str(), *id))
    }
}

impl FromIterator<(String, i32)> for FieldIds {
    fn from_iter<T: IntoIterator<Item = (String, i32)>>(ids: T) -> FieldIds {
        FieldIds {
            by_path: ids.into_iter().collect(),
        }
    }
}

impl Rejection {
    pub(crate) const ALL: [Rejection; 4] = [
        Rejection::Missing,
        Rejection::NotAccepted,
        Rejection::Unsupported,
        Rejection::ReservedName,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Rejection::Missing => "missing",
            Rejection::NotAccepted => "not_accepted",
            Rejection::Unsupported => "unsupported",
            Rejection::ReservedName => "reserved_name",
        }
    }

    pub fn from_name(name: &str) -> Option<Rejection> {
        Rejection::ALL
            .into_iter()
            .find(|rejection| rejection.name() == name)
    }
}

impl Merge {
    pub fn registry_id(&self) -> i32 {
        self.registry_id
    }

    pub fn based_on(&self) -> i32 {
        self.based_on
    }

    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    pub fn field_ids(&self) -> &FieldIds {
        &self.field_ids
    }

    pub fn identity_field_ids(&self) -> &FieldIds {
        &self.identity_field_ids
    }

    pub fn result(&self) -> MergeResult {
        let table_schema_id = self
            .schema
            .as_ref()
            .map_or(self.based_on, Schema::schema_id);

        MergeResult::Merged {
            registry_id: self.registry_id,
            table_schema_id,
            field_ids: self.field_ids.clone(),
        }
    }
}

impl TopicSchema {
    pub fn new(schema_ref: PartSchemaRef, result: MergeResult) -> TopicSchema {
        TopicSchema { schema_ref, result }
    }

    pub fn schema_ref(&self) -> PartSchemaRef {
        self.schema_ref
    }

    pub fn result(&self) -> &MergeResult {
        &self.result
    }
}

pub fn merge(
    table: &Schema,
    identity_field_ids: &FieldIds,
    part: Part,
    payload: &PayloadSchema,
    registry_id: i32,
) -> Result<Merge, Rejection> {
    merge_type(
        table,
        identity_field_ids,
        part,
        payload.payload_type(),
        registry_id,
    )
}

fn merge_type(
    table: &Schema,
    identity_field_ids: &FieldIds,
    part: Part,
    payload_type: &PayloadType,
    registry_id: i32,
) -> Result<Merge, Rejection> {
    let original = columns(table.as_struct().fields());
    let mut columns = original.clone();
    let mut state = MergeState {
        renamed_suffix: format!("_s{}", table.schema_id()),
        by_identity_path: identity_field_ids
            .iter()
            .map(|(path, id)| (path.to_owned(), FieldId::Existing(id)))
            .collect(),
        by_writer_path: BTreeMap::new(),
        new: 0,
    };

    match (part, payload_type) {
        (Part::Value, PayloadType::Struct(children)) => children.iter().try_for_each(|child| {
            state.merge_child(&mut columns, "", "", child, &RESERVED_NAMES)
        })?,
        (Part::Key, payload_type) => {
            state.merge_field(&mut columns, "", "", DECODED_KEY, payload_type, &[])?
        }
        (Part::Value, payload_type) => {
            state.merge_field(&mut columns, "", "", "value", payload_type, &RESERVED_NAMES)?
        }
    }

    let mut new_ids = NewFieldIds {
        next: highest(&columns) + 1,
        by_new: HashMap::new(),
    };
    let schema = match columns == original {
        true => None,
        false => {
            new_ids.number_struct(&mut columns);

            let built = Schema::builder()
                .with_schema_id(table.schema_id() + 1)
                .with_fields(columns.iter().map(nested))
                .build();

            Some(built.map_err(|_| Rejection::Unsupported)?)
        }
    };

    Ok(Merge {
        registry_id,
        based_on: table.schema_id(),
        schema,
        field_ids: new_ids.resolve(state.by_writer_path),
        identity_field_ids: new_ids.resolve(state.by_identity_path),
    })
}

impl FieldId {
    fn number(self) -> i32 {
        match self {
            FieldId::Existing(id) => id,
            FieldId::New(_) => unreachable!("new ids are numbered before the schema is built"),
        }
    }
}

impl MergeState {
    fn merge_child(
        &mut self,
        container: &mut Vec<Column>,
        parent: &str,
        writer_parent: &str,
        child: &PayloadField,
        reserved: &[&str],
    ) -> Result<(), Rejection> {
        let identity = child
            .identities()
            .find(|identity| {
                self.by_identity_path
                    .get(&path(parent, identity))
                    .is_some_and(|id| container.iter().any(|column| column.id == *id))
            })
            .unwrap_or(child.identity());

        self.merge_field(
            container,
            &path(parent, identity),
            &path(writer_parent, child.identity()),
            child.name(),
            child.payload_type(),
            reserved,
        )
    }

    fn merge_field(
        &mut self,
        container: &mut Vec<Column>,
        path: &str,
        writer_path: &str,
        name: &str,
        payload_type: &PayloadType,
        reserved: &[&str],
    ) -> Result<(), Rejection> {
        let existing = self
            .by_identity_path
            .get(path)
            .and_then(|id| container.iter().position(|column| column.id == *id));

        match existing {
            Some(index) if mergeable(&container[index].kind, payload_type) => {
                if container[index].name != name {
                    self.free_name(container, name, reserved)?;
                    container[index].name = name.to_owned();
                }

                self.by_writer_path
                    .insert(writer_path.to_owned(), container[index].id);

                let mut kind = container[index].kind.clone();
                self.merge_kind(&mut kind, path, writer_path, payload_type)?;
                container[index].kind = kind;

                Ok(())
            }
            Some(index) => {
                let renamed = format!("{}{}", container[index].name, self.renamed_suffix);
                self.free_name(container, &renamed, reserved)?;
                container[index].name = renamed;

                self.add(container, path, writer_path, name, payload_type, reserved)
            }
            None => self.add(container, path, writer_path, name, payload_type, reserved),
        }
    }

    fn add(
        &mut self,
        container: &mut Vec<Column>,
        path: &str,
        writer_path: &str,
        name: &str,
        payload_type: &PayloadType,
        reserved: &[&str],
    ) -> Result<(), Rejection> {
        self.free_name(container, name, reserved)?;

        let id = self.new_id(path, writer_path);
        let kind = self.new_kind(path, writer_path, payload_type);

        container.push(Column {
            id,
            name: name.to_owned(),
            required: false,
            kind,
        });

        Ok(())
    }

    fn free_name(
        &mut self,
        container: &mut [Column],
        name: &str,
        reserved: &[&str],
    ) -> Result<(), Rejection> {
        if reserved.contains(&name) {
            return Err(Rejection::ReservedName);
        }

        if let Some(holder) = container.iter_mut().find(|column| column.name == name) {
            holder.name = format!("{name}{}", self.renamed_suffix);
        }

        Ok(())
    }

    fn new_id(&mut self, path: &str, writer_path: &str) -> FieldId {
        let id = FieldId::New(self.new);
        self.new += 1;

        self.by_identity_path.insert(path.to_owned(), id);
        self.by_writer_path.insert(writer_path.to_owned(), id);

        id
    }

    fn new_kind(&mut self, node: &str, writer_path: &str, payload_type: &PayloadType) -> Kind {
        match payload_type {
            PayloadType::Primitive(primitive) => Kind::Primitive(primitive.clone()),
            PayloadType::Json => Kind::Primitive(PrimitiveType::String),
            PayloadType::Struct(children) => Kind::Struct(
                children
                    .iter()
                    .map(|child| {
                        let child_path = path(node, child.identity());
                        let child_writer = path(writer_path, child.identity());
                        let id = self.new_id(&child_path, &child_writer);

                        Column {
                            id,
                            name: child.name().to_owned(),
                            required: false,
                            kind: self.new_kind(&child_path, &child_writer, child.payload_type()),
                        }
                    })
                    .collect(),
            ),
            PayloadType::List(element) => {
                let (element_path, element_writer) =
                    (path(node, "element"), path(writer_path, "element"));
                let element_id = self.new_id(&element_path, &element_writer);

                Kind::List {
                    element_id,
                    element_required: false,
                    element: Box::new(self.new_kind(&element_path, &element_writer, element)),
                }
            }
            PayloadType::Map(key, value) => {
                let (key_path, key_writer) = (path(node, "key"), path(writer_path, "key"));
                let (value_path, value_writer) = (path(node, "value"), path(writer_path, "value"));
                let key_id = self.new_id(&key_path, &key_writer);
                let value_id = self.new_id(&value_path, &value_writer);

                Kind::Map {
                    key_id,
                    key: Box::new(self.new_kind(&key_path, &key_writer, key)),
                    value_id,
                    value_required: false,
                    value: Box::new(self.new_kind(&value_path, &value_writer, value)),
                }
            }
        }
    }

    fn merge_kind(
        &mut self,
        kind: &mut Kind,
        node: &str,
        writer_path: &str,
        payload_type: &PayloadType,
    ) -> Result<(), Rejection> {
        match (kind, payload_type) {
            (Kind::Primitive(column), PayloadType::Primitive(written)) => {
                if promotable(column, written) {
                    *column = written.clone();
                }

                Ok(())
            }
            (Kind::Primitive(_), PayloadType::Json) => Ok(()),
            (Kind::Struct(children), PayloadType::Struct(written)) => written
                .iter()
                .try_for_each(|child| self.merge_child(children, node, writer_path, child, &[])),
            (
                Kind::List {
                    element_id,
                    element,
                    ..
                },
                PayloadType::List(written),
            ) => {
                let (element_path, element_writer) =
                    (path(node, "element"), path(writer_path, "element"));
                self.by_writer_path
                    .insert(element_writer.clone(), *element_id);

                self.merge_kind(element, &element_path, &element_writer, written)
            }
            (
                Kind::Map {
                    key_id,
                    key,
                    value_id,
                    value,
                    ..
                },
                PayloadType::Map(written_key, written_value),
            ) => {
                let (key_path, key_writer) = (path(node, "key"), path(writer_path, "key"));
                let (value_path, value_writer) = (path(node, "value"), path(writer_path, "value"));
                self.by_writer_path.insert(key_writer.clone(), *key_id);
                self.by_writer_path.insert(value_writer.clone(), *value_id);

                self.merge_kind(key, &key_path, &key_writer, written_key)?;
                self.merge_kind(value, &value_path, &value_writer, written_value)
            }
            _ => unreachable!("merge_field merges only mergeable kinds"),
        }
    }
}

impl NewFieldIds {
    fn number_struct(&mut self, columns: &mut [Column]) {
        for column in columns.iter_mut() {
            self.number(&mut column.id);
        }

        for column in columns.iter_mut() {
            self.number_kind(&mut column.kind);
        }
    }

    fn number_kind(&mut self, kind: &mut Kind) {
        match kind {
            Kind::Primitive(_) => {}
            Kind::Struct(children) => self.number_struct(children),
            Kind::List {
                element_id,
                element,
                ..
            } => {
                self.number(element_id);
                self.number_kind(element);
            }
            Kind::Map {
                key_id,
                key,
                value_id,
                value,
                ..
            } => {
                self.number(key_id);
                self.number(value_id);
                self.number_kind(key);
                self.number_kind(value);
            }
        }
    }

    fn number(&mut self, id: &mut FieldId) {
        if let FieldId::New(new) = *id {
            self.by_new.insert(new, self.next);
            *id = FieldId::Existing(self.next);
            self.next += 1;
        }
    }

    fn resolve(&self, ids: BTreeMap<String, FieldId>) -> FieldIds {
        ids.into_iter()
            .map(|(path, id)| {
                let number = match id {
                    FieldId::Existing(id) => id,
                    FieldId::New(new) => self.by_new[&new],
                };

                (path, number)
            })
            .collect()
    }
}

fn mergeable(kind: &Kind, payload_type: &PayloadType) -> bool {
    match (kind, payload_type) {
        (Kind::Primitive(column), PayloadType::Primitive(written)) => {
            column == written || promotable(column, written) || promotable(written, column)
        }
        (Kind::Primitive(column), PayloadType::Json) => *column == PrimitiveType::String,
        (Kind::Struct(_), PayloadType::Struct(_)) => true,
        (Kind::List { element, .. }, PayloadType::List(written)) => mergeable(element, written),
        (Kind::Map { key, value, .. }, PayloadType::Map(written_key, written_value)) => {
            mergeable(key, written_key) && mergeable(value, written_value)
        }
        _ => false,
    }
}

fn promotable(from: &PrimitiveType, to: &PrimitiveType) -> bool {
    match (from, to) {
        (PrimitiveType::Int, PrimitiveType::Long)
        | (PrimitiveType::Float, PrimitiveType::Double) => true,
        (
            PrimitiveType::Decimal { precision, scale },
            PrimitiveType::Decimal {
                precision: wider,
                scale: same,
            },
        ) => scale == same && wider > precision,
        _ => false,
    }
}

fn columns(fields: &[NestedFieldRef]) -> Vec<Column> {
    fields
        .iter()
        .map(|field| Column {
            id: FieldId::Existing(field.id),
            name: field.name.clone(),
            required: field.required,
            kind: kind(&field.field_type),
        })
        .collect()
}

fn kind(field_type: &Type) -> Kind {
    match field_type {
        Type::Primitive(primitive) => Kind::Primitive(primitive.clone()),
        Type::Struct(structure) => Kind::Struct(columns(structure.fields())),
        Type::List(list) => Kind::List {
            element_id: FieldId::Existing(list.element_field.id),
            element_required: list.element_field.required,
            element: Box::new(kind(&list.element_field.field_type)),
        },
        Type::Map(map) => Kind::Map {
            key_id: FieldId::Existing(map.key_field.id),
            key: Box::new(kind(&map.key_field.field_type)),
            value_id: FieldId::Existing(map.value_field.id),
            value_required: map.value_field.required,
            value: Box::new(kind(&map.value_field.field_type)),
        },
    }
}

fn highest(columns: &[Column]) -> i32 {
    columns
        .iter()
        .map(|column| existing_id(column.id).max(highest_in(&column.kind)))
        .max()
        .unwrap_or(0)
}

fn highest_in(kind: &Kind) -> i32 {
    match kind {
        Kind::Primitive(_) => 0,
        Kind::Struct(children) => highest(children),
        Kind::List {
            element_id,
            element,
            ..
        } => existing_id(*element_id).max(highest_in(element)),
        Kind::Map {
            key_id,
            key,
            value_id,
            value,
            ..
        } => existing_id(*key_id)
            .max(existing_id(*value_id))
            .max(highest_in(key))
            .max(highest_in(value)),
    }
}

fn existing_id(id: FieldId) -> i32 {
    match id {
        FieldId::Existing(id) => id,
        FieldId::New(_) => 0,
    }
}

fn nested(column: &Column) -> NestedFieldRef {
    NestedField::new(
        column.id.number(),
        &column.name,
        field_type(&column.kind),
        column.required,
    )
    .into()
}

fn field_type(kind: &Kind) -> Type {
    match kind {
        Kind::Primitive(primitive) => Type::Primitive(primitive.clone()),
        Kind::Struct(children) => {
            Type::Struct(StructType::new(children.iter().map(nested).collect()))
        }
        Kind::List {
            element_id,
            element_required,
            element,
        } => Type::List(ListType::new(
            NestedField::list_element(element_id.number(), field_type(element), *element_required)
                .into(),
        )),
        Kind::Map {
            key_id,
            key,
            value_id,
            value_required,
            value,
        } => Type::Map(MapType::new(
            NestedField::map_key_element(key_id.number(), field_type(key)).into(),
            NestedField::map_value_element(value_id.number(), field_type(value), *value_required)
                .into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::NestedFieldRef;
    use proptest::prelude::*;

    use super::*;
    use crate::binary_schema;

    fn field(name: &str, payload_type: PayloadType) -> PayloadField {
        PayloadField::new(name, name, Vec::new(), payload_type)
    }

    fn primitive(primitive: PrimitiveType) -> PayloadType {
        PayloadType::Primitive(primitive)
    }

    fn merged(table: &Schema, field_ids: &FieldIds, payload_type: &PayloadType) -> Merge {
        merge_type(table, field_ids, Part::Value, payload_type, 1).unwrap()
    }

    fn typed(schema: &Schema) -> Vec<(i32, String, Type)> {
        schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| field.id >= 10)
            .map(|field| (field.id, field.name.clone(), (*field.field_type).clone()))
            .collect()
    }

    #[test]
    fn incompatible_changes_keep_the_old_column_under_a_new_name() {
        let first = PayloadType::Struct(vec![field("a", primitive(PrimitiveType::Int))]);
        let second = PayloadType::Struct(vec![field("a", primitive(PrimitiveType::String))]);

        let one = merged(binary_schema(), &FieldIds::default(), &first);
        let one_schema = one.schema().unwrap().clone();
        let two = merged(&one_schema, one.identity_field_ids(), &second);

        let two_schema = two.schema().unwrap();
        assert_eq!(
            typed(two_schema),
            [
                (10, "a_s1".to_owned(), Type::Primitive(PrimitiveType::Int)),
                (11, "a".to_owned(), Type::Primitive(PrimitiveType::String)),
            ]
        );
        assert_eq!(two.identity_field_ids().get("a"), Some(11));
        assert_eq!(two.field_ids().get("a"), Some(11));
        assert_eq!(one.field_ids().get("a"), Some(10));
        assert_eq!(
            two.result(),
            MergeResult::Merged {
                registry_id: 1,
                table_schema_id: 2,
                field_ids: two.field_ids().clone(),
            }
        );
    }

    #[test]
    fn rejection_names_round_trip() {
        for rejection in Rejection::ALL {
            assert_eq!(Rejection::from_name(rejection.name()), Some(rejection));
        }

        assert_eq!(Rejection::from_name("absent"), None);
    }

    #[test]
    fn reserved_names_reject_the_schema() {
        let mink =
            PayloadType::Struct(vec![field("_kafka_offset", primitive(PrimitiveType::Long))]);
        let iceberg = PayloadType::Struct(vec![field("_partition", primitive(PrimitiveType::Int))]);
        let key = PayloadType::Struct(vec![field("_kafka_offset", primitive(PrimitiveType::Long))]);
        let none = FieldIds::default();

        let mink = merge_type(binary_schema(), &none, Part::Value, &mink, 1);
        let iceberg = merge_type(binary_schema(), &none, Part::Value, &iceberg, 1);
        let key = merge_type(binary_schema(), &none, Part::Key, &key, 1);

        assert_eq!(mink, Err(Rejection::ReservedName));
        assert_eq!(iceberg, Err(Rejection::ReservedName));
        assert!(key.is_ok());
    }

    #[test]
    fn names_iceberg_cannot_tell_apart_reject_the_schema() {
        let nested = PayloadType::Struct(vec![field("b", primitive(PrimitiveType::Long))]);
        let clashing = PayloadType::Struct(vec![
            field("a", nested),
            field("a.b", primitive(PrimitiveType::Long)),
        ]);

        let merged = merge_type(
            binary_schema(),
            &FieldIds::default(),
            Part::Value,
            &clashing,
            1,
        );

        assert_eq!(merged, Err(Rejection::Unsupported));
    }

    fn struct_of(fields: BTreeMap<String, PayloadType>) -> PayloadType {
        let fields = fields
            .into_iter()
            .map(|(name, payload_type)| field(&name, payload_type))
            .collect();

        PayloadType::Struct(fields)
    }

    fn payload_type() -> impl Strategy<Value = PayloadType> {
        let leaf = prop_oneof![
            Just(primitive(PrimitiveType::Int)),
            Just(primitive(PrimitiveType::Long)),
            Just(primitive(PrimitiveType::Float)),
            Just(primitive(PrimitiveType::Double)),
            Just(primitive(PrimitiveType::String)),
            Just(PayloadType::Json),
        ];

        leaf.prop_recursive(2, 8, 3, |inner| {
            prop_oneof![
                inner
                    .clone()
                    .prop_map(|element| PayloadType::List(Box::new(element))),
                proptest::collection::btree_map("[a-c]", inner, 1..3).prop_map(struct_of),
            ]
        })
    }

    fn version() -> impl Strategy<Value = PayloadType> {
        proptest::collection::btree_map("[a-e]", payload_type(), 1..5).prop_map(struct_of)
    }

    fn ids(fields: &[NestedFieldRef], into: &mut BTreeMap<i32, Type>) {
        for field in fields {
            into.insert(field.id, (*field.field_type).clone());

            match field.field_type.as_ref() {
                Type::Struct(structure) => ids(structure.fields(), into),
                Type::List(list) => ids(std::slice::from_ref(&list.element_field), into),
                Type::Map(map) => ids(&[map.key_field.clone(), map.value_field.clone()], into),
                Type::Primitive(_) => {}
            }
        }
    }

    proptest! {
        #[test]
        fn schemas_only_grow_and_ids_are_never_reused(versions in proptest::collection::vec(version(), 1..6)) {
            let mut latest = binary_schema().clone();
            let mut fields = FieldIds::default();

            for payload_type in &versions {
                let step = merged(&latest, &fields, payload_type);
                let next = step.schema().cloned().unwrap_or_else(|| latest.clone());

                let (mut before, mut after) = (BTreeMap::new(), BTreeMap::new());
                ids(latest.as_struct().fields(), &mut before);
                ids(next.as_struct().fields(), &mut after);
                for (id, kind) in &before {
                    let kept = &after[id];
                    let promoted = matches!(
                        (kind, kept),
                        (Type::Primitive(from), Type::Primitive(to)) if promotable(from, to)
                    );
                    let nested = !matches!(kind, Type::Primitive(_));

                    prop_assert!(kept == kind || promoted || nested, "{id}: {kind:?} became {kept:?}");
                }

                let again = merged(&next, step.identity_field_ids(), payload_type);
                prop_assert!(again.schema().is_none());
                prop_assert_eq!(again.identity_field_ids(), step.identity_field_ids());

                latest = next;
                fields = step.identity_field_ids().clone();
            }
        }
    }
}
