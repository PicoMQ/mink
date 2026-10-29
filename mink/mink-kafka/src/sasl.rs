use std::fmt;
use std::time::Duration;

use crate::Error;
use crate::connection::Connection;
use crate::scram::{Hash, Scram};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Sasl {
    mechanism: Mechanism,
    username: String,
    password: String,
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

    pub(crate) async fn authenticate(
        &self,
        connection: &mut Connection,
    ) -> Result<Option<Duration>, Error> {
        connection.sasl_handshake(self.mechanism).await?;

        let lifetime_ms = match self.mechanism {
            Mechanism::Plain => self.plain(connection).await?,
            Mechanism::ScramSha256 => self.scram(connection, Hash::Sha256).await?,
            Mechanism::ScramSha512 => self.scram(connection, Hash::Sha512).await?,
        };

        Ok(u64::try_from(lifetime_ms)
            .ok()
            .filter(|lifetime_ms| *lifetime_ms > 0)
            .map(Duration::from_millis))
    }

    async fn plain(&self, connection: &mut Connection) -> Result<i64, Error> {
        let message = format!("\0{}\0{}", self.username, self.password);

        let response = connection.sasl_authenticate(message.into_bytes()).await?;

        Ok(response.session_lifetime_ms)
    }

    async fn scram(&self, connection: &mut Connection, hash: Hash) -> Result<i64, Error> {
        let scram = Scram::new(hash, &self.username)?;

        let server_first = connection
            .sasl_authenticate(scram.first_message().into_bytes())
            .await?;
        let (message, signature) =
            scram.final_message(&self.password, text(&server_first.auth_bytes)?)?;
        let server_final = connection.sasl_authenticate(message.into_bytes()).await?;
        signature.verify(text(&server_final.auth_bytes)?)?;

        Ok(server_final.session_lifetime_ms)
    }
}

impl fmt::Debug for Sasl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Sasl")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
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
    fn debug_hides_the_password() {
        let sasl = Sasl::new(Mechanism::Plain, "mink".to_owned(), "hunter2".to_owned());

        let shown = format!("{sasl:?}");

        assert!(shown.contains("mink"));
        assert!(!shown.contains("hunter2"));
    }
}
