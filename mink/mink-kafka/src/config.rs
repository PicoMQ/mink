use std::path::Path;
use std::time::Duration;

use crate::Error;
use crate::connection::MAX_FRAME;
use crate::sasl::Sasl;
use crate::tls::Tls;

const MAX_WAIT: Duration = Duration::from_millis(500);
const MAX_BYTES: i32 = 8 * 1024 * 1024;
const RETRY_DEADLINE: Duration = Duration::from_secs(60);
const RESPONSE_MARGIN: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Config {
    bootstrap: Vec<String>,
    max_wait: Duration,
    max_bytes: i32,
    retry_deadline: Duration,
    tls: Option<Tls>,
    sasl: Option<Sasl>,
}

impl Config {
    pub fn new(bootstrap: Vec<String>) -> Result<Config, Error> {
        if bootstrap.is_empty() {
            return Err(Error::EmptyBootstrap);
        }

        Ok(Config {
            bootstrap,
            max_wait: MAX_WAIT,
            max_bytes: MAX_BYTES,
            retry_deadline: RETRY_DEADLINE,
            tls: None,
            sasl: None,
        })
    }

    pub fn with_tls(mut self, ca: Option<&Path>) -> Result<Config, Error> {
        self.tls = Some(Tls::new(ca)?);
        Ok(self)
    }

    pub fn with_sasl(mut self, sasl: Sasl) -> Config {
        self.sasl = Some(sasl);
        self
    }

    pub fn with_max_wait(mut self, max_wait: Duration) -> Result<Config, Error> {
        let Some(max_wait_ms) = whole_millis(max_wait) else {
            return Err(Error::MaxWait(max_wait));
        };

        self.max_wait = Duration::from_millis(max_wait_ms.unsigned_abs().into());
        Ok(self)
    }

    pub fn with_max_bytes(mut self, max_bytes: i32) -> Result<Config, Error> {
        if !(1..=MAX_FRAME).contains(&max_bytes) {
            return Err(Error::MaxBytes(max_bytes));
        }

        self.max_bytes = max_bytes;
        Ok(self)
    }

    pub fn with_retry_deadline(mut self, retry_deadline: Duration) -> Config {
        self.retry_deadline = retry_deadline;
        self
    }

    pub(crate) fn bootstrap(&self) -> &[String] {
        &self.bootstrap
    }

    pub(crate) fn max_bytes(&self) -> i32 {
        self.max_bytes
    }

    pub(crate) fn retry_deadline(&self) -> Duration {
        self.retry_deadline
    }

    pub(crate) fn max_wait_ms(&self) -> i32 {
        whole_millis(self.max_wait).expect("with_max_wait checks the range")
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.max_wait + RESPONSE_MARGIN
    }

    pub(crate) fn tls(&self) -> Option<&Tls> {
        self.tls.as_ref()
    }

    pub(crate) fn sasl(&self) -> Option<&Sasl> {
        self.sasl.as_ref()
    }
}

fn whole_millis(wait: Duration) -> Option<i32> {
    i32::try_from(wait.as_millis()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_ones_the_spec_names() {
        let config = Config::new(vec!["localhost:9092".to_owned()]).unwrap();

        assert_eq!(config.max_wait_ms(), 500);
        assert_eq!(config.max_bytes(), 8 * 1024 * 1024);
        assert_eq!(config.retry_deadline(), Duration::from_secs(60));
        assert_eq!(config.request_timeout(), Duration::from_millis(30_500));
    }

    #[test]
    fn settings_outside_their_limits_are_rejected() {
        let config = Config::new(vec!["localhost:9092".to_owned()]).unwrap();
        let too_long = Duration::from_millis(i32::MAX as u64 + 1);

        let empty = Config::new(Vec::new());
        let zero = config.clone().with_max_bytes(0);
        let over_frame = config.clone().with_max_bytes(MAX_FRAME + 1);
        let long_wait = config.with_max_wait(too_long);

        assert!(matches!(empty, Err(Error::EmptyBootstrap)));
        assert!(matches!(zero, Err(Error::MaxBytes(0))));
        assert!(matches!(over_frame, Err(Error::MaxBytes(_))));
        assert!(matches!(long_wait, Err(Error::MaxWait(_))));
    }
}
