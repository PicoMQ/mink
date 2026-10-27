use iceberg::spec::PrimitiveType;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PayloadType {
    Primitive(PrimitiveType),
    Json,
    Struct(Vec<PayloadField>),
    List(Box<PayloadType>),
    Map(Box<PayloadType>, Box<PayloadType>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PayloadField {
    name: String,
    identity: String,
    aliases: Vec<String>,
    payload_type: PayloadType,
}

impl PayloadField {
    pub(crate) fn new(
        name: impl Into<String>,
        identity: impl Into<String>,
        aliases: Vec<String>,
        payload_type: PayloadType,
    ) -> PayloadField {
        PayloadField {
            name: name.into(),
            identity: identity.into(),
            aliases,
            payload_type,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn identity(&self) -> &str {
        &self.identity
    }

    pub(crate) fn identities(&self) -> impl Iterator<Item = &str> {
        [self.identity.as_str()]
            .into_iter()
            .chain(self.aliases.iter().map(String::as_str))
    }

    pub(crate) fn payload_type(&self) -> &PayloadType {
        &self.payload_type
    }
}

pub(crate) fn path(parent: &str, child: &str) -> String {
    match parent.is_empty() {
        true => child.to_owned(),
        false => format!("{parent}.{child}"),
    }
}
