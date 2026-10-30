use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls_platform_verifier::Verifier;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};

use crate::Error;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    ca: Option<PathBuf>,
}

impl Tls {
    pub fn new(ca: Option<PathBuf>) -> Tls {
        Tls { ca }
    }

    pub(crate) fn connector(&self) -> Result<TlsConnector, Error> {
        let provider = Arc::new(aws_lc_rs::default_provider());
        let verifier = match &self.ca {
            Some(ca) => Verifier::new_with_extra_roots(certificates(ca)?, provider.clone())?,
            None => Verifier::new(provider.clone())?,
        };

        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();

        Ok(TlsConnector::from(Arc::new(config)))
    }
}

pub(crate) async fn connect(
    connector: &TlsConnector,
    address: &str,
    stream: TcpStream,
) -> Result<TlsStream<TcpStream>, Error> {
    let name = ServerName::try_from(host(address).to_owned())
        .map_err(|_| Error::ServerName(address.to_owned()))?;

    connector.connect(name, stream).await.map_err(|error| {
        match error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        {
            Some(rejected) => Error::Tls(rejected.clone()),
            None => Error::Io(error),
        }
    })
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

        let missing = Tls::new(Some(PathBuf::from("/nonexistent/ca.pem"))).connector();
        let without = Tls::new(Some(empty.clone())).connector();

        assert!(
            matches!(missing, Err(Error::CaFile { .. })),
            "{:?}",
            missing.err()
        );
        assert!(
            matches!(without, Err(Error::CaFile { .. })),
            "{:?}",
            without.err()
        );
        std::fs::remove_file(empty).unwrap();
    }
}
