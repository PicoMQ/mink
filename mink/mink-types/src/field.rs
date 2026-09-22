//! A named, typed field with an optional description and optional stable id.

use crate::{DataType, Error};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FieldId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Field {
    name: String,
    data_type: DataType,
    description: Option<String>,
    id: Option<FieldId>,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Result<Self, Error> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::BlankFieldName);
        }

        Ok(Field {
            name,
            data_type,
            description: None,
            id: None,
        })
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Result<Self, Error> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::BlankFieldName);
        }
        self.name = name;
        Ok(self)
    }

    pub fn with_id(mut self, id: FieldId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_data_type(mut self, data_type: DataType) -> Self {
        self.data_type = data_type;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn id(&self) -> Option<FieldId> {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_must_not_be_blank() {
        assert_eq!(
            Field::new("", DataType::int()).unwrap_err(),
            Error::BlankFieldName
        );
        assert_eq!(
            Field::new(" \t", DataType::int()).unwrap_err(),
            Error::BlankFieldName
        );
        assert!(Field::new(" a ", DataType::int()).is_ok());
    }

    #[test]
    fn builder_sets_optional_parts() {
        let field = Field::new("id", DataType::big_int())
            .unwrap()
            .with_description("primary key")
            .with_id(FieldId(7));
        assert_eq!(field.description(), Some("primary key"));
        assert_eq!(field.id(), Some(FieldId(7)));
    }
}
