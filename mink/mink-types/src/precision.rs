//! Fractional-second precision for time and timestamp types, from seconds to nanoseconds.

use std::fmt;

use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Precision(u8);

impl Precision {
    pub const SECONDS: Precision = Precision(0);
    pub const MILLIS: Precision = Precision(3);
    pub const MICROS: Precision = Precision(6);
    pub const NANOS: Precision = Precision(9);
    pub const MAX: Precision = Precision::NANOS;

    pub fn new(precision: u8) -> Result<Self, Error> {
        if precision <= Self::MAX.0 {
            Ok(Precision(precision))
        } else {
            Err(Error::Precision(precision))
        }
    }

    pub const fn get(self) -> u8 {
        self.0
    }
}

impl fmt::Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds() {
        assert_eq!(Precision::new(0), Ok(Precision::SECONDS));
        assert_eq!(Precision::new(9), Ok(Precision::NANOS));
        assert_eq!(Precision::new(10), Err(Error::Precision(10)));
    }
}
