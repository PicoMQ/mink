use std::fmt;
use std::path::Path;
use std::sync::Arc;

use rustls_platform_verifier::Verifier;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};

use crate::Error;

#[derive(Clone)]
pub(crate) struct Tls {
    connector: TlsConnector,
}

impl Tls {
    pub(crate) fn new(ca: Option<&Path>) -> Result<Tls, Error> {
        let provider = Arc::new(aws_lc_rs::default_provider());
        let verifier = match ca {
            Some(ca) => Verifier::new_with_extra_roots(certificates(ca)?, provider.clone())?,
            None => Verifier::new(provider.clone())?,
        };

        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();

        Ok(Tls {
            connector: TlsConnector::from(Arc::new(config)),
        })
    }

    pub(crate) async fn connect(
        &self,
        address: &str,
        stream: TcpStream,
    ) -> Result<TlsStream<TcpStream>, Error> {
        let name = ServerName::try_from(host(address).to_owned())
            .map_err(|_| Error::ServerName(address.to_owned()))?;

        self.connector.connect(name, stream).await.map_err(|error| {
            match error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<rustls::Error>())
            {
                Some(rejected) => Error::Tls(rejected.clone()),
                None => Error::Io(error),
            }
        })
    }
}

impl fmt::Debug for Tls {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Tls").finish_non_exhaustive()
    }
}

fn certificates(ca: &Path) -> Result<Vec<CertificateDer<'static>>, Error> {
    let unreadable = |reason: String| Error::CaFile {
        path: ca.to_owned(),
        reason,
    };

    let certificates = CertificateDer::pem_file_iter(ca)
        .map_err(|error| unreadable(error.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| unreadable(error.to_string()))?;
    if certificates.is_empty() {
        return Err(unreadable("it holds no certificates".to_owned()));
    }

    Ok(certificates)
}

fn host(address: &str) -> &str {
    let host = address.rsplit_once(':').map_or(address, |(host, _)| host);

    host.trim_start_matches('[').trim_end_matches(']')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_server_name_is_the_host_of_the_address() {
        assert_eq!(
            host("b-1.mink.kafka.us-east-1.amazonaws.com:9096"),
            "b-1.mink.kafka.us-east-1.amazonaws.com"
        );
        assert_eq!(host("10.0.0.1:9093"), "10.0.0.1");
        assert_eq!(host("[::1]:9093"), "::1");
    }

    #[test]
    fn a_ca_file_without_certificates_is_rejected() {
        let empty = std::env::temp_dir().join(format!("mink-empty-{}.pem", std::process::id()));
        std::fs::write(&empty, "").unwrap();

        let missing = Tls::new(Some(Path::new("/nonexistent/ca.pem")));
        let without = Tls::new(Some(&empty));

        assert!(matches!(missing, Err(Error::CaFile { .. })), "{missing:?}");
        assert!(matches!(without, Err(Error::CaFile { .. })), "{without:?}");
        std::fs::remove_file(empty).unwrap();
    }
}
