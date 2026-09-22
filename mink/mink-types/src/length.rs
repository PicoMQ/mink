//! Character or byte length of fixed-width types, constrained to a positive 31-bit range.

use std::fmt;
use std::num::NonZeroU32;

use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Length(NonZeroU32);

impl Length {
    pub const MIN: Length = Length(NonZeroU32::MIN);
    pub const MAX: Length = Length(NonZeroU32::new(i32::MAX as u32).unwrap());

    pub fn new(length: u32) -> Result<Self, Error> {
        NonZeroU32::new(length)
            .filter(|n| *n <= Self::MAX.0)
            .map(Length)
            .ok_or(Error::Length)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Default for Length {
    fn default() -> Self {
        Length::MIN
    }
}

impl fmt::Display for Length {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds() {
        assert_eq!(Length::new(0), Err(Error::Length));
        assert_eq!(Length::new(1), Ok(Length::MIN));
        assert_eq!(Length::new(i32::MAX as u32), Ok(Length::MAX));
        assert_eq!(Length::new(i32::MAX as u32 + 1), Err(Error::Length));
        assert_eq!(Length::new(u32::MAX), Err(Error::Length));
    }

    #[test]
    fn default_is_one() {
        assert_eq!(Length::default().get(), 1);
    }
}
