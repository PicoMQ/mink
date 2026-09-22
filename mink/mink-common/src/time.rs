//! Millisecond wall-clock abstraction with a real and a manually driven implementation for tests,
//! plus human-readable formatting of timestamps and durations.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn millis(&self) -> i64;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn millis(&self) -> i64 {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
    }
}

#[derive(Debug, Default)]
pub struct ManualClock {
    millis: AtomicI64,
}

impl ManualClock {
    pub fn new(millis: i64) -> Self {
        ManualClock {
            millis: AtomicI64::new(millis),
        }
    }

    pub fn set(&self, millis: i64) {
        self.millis.store(millis, Ordering::SeqCst);
    }

    pub fn advance(&self, by: Duration) {
        let by = i64::try_from(by.as_millis()).unwrap_or(i64::MAX);
        self.millis.fetch_add(by, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn millis(&self) -> i64 {
        self.millis.load(Ordering::SeqCst)
    }
}

pub fn when(ms: i64) -> String {
    if ms <= 0 {
        return "-".to_owned();
    }

    let secs = ms.div_euclid(1000) as u64;
    let nanos = (ms.rem_euclid(1000) * 1_000_000) as u32;
    let time = UNIX_EPOCH + Duration::new(secs, nanos);
    humantime::format_rfc3339_millis(time).to_string()
}

pub fn duration(ms: i64) -> String {
    humantime::format_duration(Duration::from_millis(ms.max(0) as u64)).to_string()
}

pub fn parse_duration(text: &str) -> Option<Duration> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: u64 = number.parse().ok()?;

    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "ms" => 1,
        "s" | "sec" => 1_000,
        "m" | "min" => 60_000,
        "h" | "hour" => 3_600_000,
        "d" | "day" => 86_400_000,
        _ => return None,
    };

    number.checked_mul(multiplier).map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_moves_only_when_told() {
        let clock = ManualClock::new(10);
        assert_eq!(clock.millis(), 10);
        clock.advance(Duration::from_secs(2));
        assert_eq!(clock.millis(), 2010);
        clock.set(5);
        assert_eq!(clock.millis(), 5);
    }

    #[test]
    fn system_clock_is_after_2020() {
        assert!(SystemClock.millis() > 1_577_836_800_000);
    }

    #[test]
    fn parses_bare_millis_and_units() {
        assert_eq!(parse_duration("1500"), Some(Duration::from_millis(1500)));
        assert_eq!(parse_duration("3 min"), Some(Duration::from_secs(180)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_duration("soon"), None);
        assert_eq!(parse_duration(""), None);
    }
}
