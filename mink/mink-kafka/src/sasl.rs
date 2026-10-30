use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::Error;
use crate::scram::{Hash, Scram, ServerSignature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sasl {
    mechanism: Mechanism,
    username: String,
    password: String,
}

pub(crate) enum Exchange {
    Plain,
    ScramFirst { scram: Scram, password: String },
    ScramFinal(ServerSignature),
}

impl Mechanism {
    const ALL: [Mechanism; 3] = [
        Mechanism::Plain,
        Mechanism::ScramSha256,
        Mechanism::ScramSha512,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Mechanism::Plain => "PLAIN",
            Mechanism::ScramSha256 => "SCRAM-SHA-256",
            Mechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    pub fn from_name(name: &str) -> Option<Mechanism> {
        Mechanism::ALL
            .into_iter()
            .find(|mechanism| mechanism.name() == name)
    }
}

impl Sasl {
    pub fn new(mechanism: Mechanism, username: String, password: String) -> Sasl {
        Sasl {
            mechanism,
            username,
            password,
        }
    }

    pub fn mechanism(&self) -> Mechanism {
        self.mechanism
    }

    pub(crate) fn start(&self) -> Result<(Exchange, Vec<u8>), Error> {
        let hash = match self.mechanism {
            Mechanism::Plain => {
                let message = format!("\0{}\0{}", self.username, self.password);
                return Ok((Exchange::Plain, message.into_bytes()));
            }
            Mechanism::ScramSha256 => Hash::Sha256,
            Mechanism::ScramSha512 => Hash::Sha512,
        };

        let scram = Scram::new(hash, &self.username)?;
        let message = scram.first_message().into_bytes();
        let exchange = Exchange::ScramFirst {
            scram,
            password: self.password.clone(),
        };

        Ok((exchange, message))
    }
}

impl Exchange {
    pub(crate) fn respond(self, server: &[u8]) -> Result<Option<(Exchange, Vec<u8>)>, Error> {
        match self {
            Exchange::Plain => Ok(None),
            Exchange::ScramFirst { scram, password } => {
                let (message, signature) = scram.final_message(&password, text(server)?)?;

                Ok(Some((
                    Exchange::ScramFinal(signature),
                    message.into_bytes(),
                )))
            }
            Exchange::ScramFinal(signature) => {
                signature.verify(text(server)?)?;

                Ok(None)
            }
        }
    }
}

impl Serialize for Mechanism {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for Mechanism {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Mechanism, D::Error> {
        let name = String::deserialize(deserializer)?;

        Mechanism::from_name(&name).ok_or_else(|| {
            de::Error::custom(format!(
                "{name:?} is not a SASL mechanism: PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512"
            ))
        })
    }
}

fn text(bytes: &[u8]) -> Result<&str, Error> {
    std::str::from_utf8(bytes).map_err(|_| Error::Scram("the server message is not utf-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mechanisms_use_kafka_names() {
        let names: Vec<_> = Mechanism::ALL.into_iter().map(Mechanism::name).collect();

        assert_eq!(names, ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]);
        assert_eq!(
            Mechanism::from_name("SCRAM-SHA-512"),
            Some(Mechanism::ScramSha512)
        );
        assert_eq!(Mechanism::from_name("scram-sha-512"), None);
    }

    #[test]
    fn plain_sends_one_message_and_finishes_on_the_reply() {
        let sasl = Sasl::new(Mechanism::Plain, "mink".to_owned(), "secret".to_owned());

        let (exchange, message) = sasl.start().unwrap();
        let next = exchange.respond(b"").unwrap();

        assert_eq!(message, b"\0mink\0secret");
        assert!(next.is_none());
    }
}
