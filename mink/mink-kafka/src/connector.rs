use std::time::{Duration, Instant};

use mink_metrics::Metrics;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::Error;
use crate::config::Config;
use crate::connection::{Connection, Transport};
use crate::sasl::Sasl;
use crate::tls;

const SESSION_SHARE: f64 = 0.9;

pub(crate) struct Connector {
    tls: Option<TlsConnector>,
    sasl: Option<Sasl>,
    timeout: Duration,
}

impl Connector {
    pub(crate) fn new(config: &Config) -> Result<Connector, Error> {
        let tls = config.tls().map(|tls| tls.connector()).transpose()?;

        Ok(Connector {
            tls,
            sasl: config.sasl().cloned(),
            timeout: config.request_timeout(),
        })
    }

    pub(crate) async fn connect(
        &self,
        address: &str,
        metrics: Metrics,
    ) -> Result<Connection, Error> {
        let opened = tokio::time::timeout(self.timeout, self.stream(address))
            .await
            .map_err(|_| Error::Timeout)??;
        let mut connection = Connection::open(opened, self.timeout, metrics).await?;

        if let Some(sasl) = &self.sasl {
            let lifetime = authenticate(&mut connection, sasl).await?;
            if let Some(lifetime) = lifetime {
                connection.expire_at(Instant::now() + lifetime.mul_f64(SESSION_SHARE));
            }
        }

        Ok(connection)
    }

    async fn stream(&self, address: &str) -> Result<Box<dyn Transport>, Error> {
        let tcp = TcpStream::connect(address).await?;
        tcp.set_nodelay(true)?;

        match &self.tls {
            Some(connector) => Ok(Box::new(tls::connect(connector, address, tcp).await?)),
            None => Ok(Box::new(tcp)),
        }
    }
}

async fn authenticate(connection: &mut Connection, sasl: &Sasl) -> Result<Option<Duration>, Error> {
    connection.sasl_handshake(sasl.mechanism()).await?;

    let (mut exchange, mut message) = sasl.start()?;
    loop {
        let response = connection.sasl_authenticate(message).await?;

        match exchange.respond(&response.auth_bytes)? {
            Some((next, reply)) => (exchange, message) = (next, reply),
            None => {
                let lifetime_ms = u64::try_from(response.session_lifetime_ms).unwrap_or(0);
                return Ok((lifetime_ms > 0).then(|| Duration::from_millis(lifetime_ms)));
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use kafka_protocol::messages::{ApiKey, SaslAuthenticateResponse, SaslHandshakeResponse};

    use super::*;
    use crate::connection::tests::{TIMEOUT, broker, frame};
    use crate::sasl::Mechanism;

    pub(crate) fn plain(timeout: Duration) -> Connector {
        Connector {
            tls: None,
            sasl: None,
            timeout,
        }
    }

    #[tokio::test]
    async fn a_connection_expires_before_its_sasl_session_does() {
        let address = broker(|key, version, correlation, _| match key {
            ApiKey::SaslHandshake => Some(frame(
                correlation,
                key,
                version,
                &SaslHandshakeResponse::default(),
            )),
            ApiKey::SaslAuthenticate => Some(frame(
                correlation,
                key,
                version,
                &SaslAuthenticateResponse::default().with_session_lifetime_ms(1_000),
            )),
            _ => None,
        })
        .await;
        let connector = Connector {
            sasl: Some(Sasl::new(
                Mechanism::Plain,
                "mink".to_owned(),
                "mink".to_owned(),
            )),
            ..plain(TIMEOUT)
        };

        let connection = connector.connect(&address, Metrics::new()).await.unwrap();
        let fresh = connection.expired();
        tokio::time::sleep(Duration::from_millis(950)).await;

        assert!(!fresh);
        assert!(connection.expired());
        assert!(!connection.closed());
    }
}
