//! Decimal precision and scale as a validated pair.

use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    precision: u8,
    scale: u8,
}

impl Decimal {
    pub const MIN_PRECISION: u8 = 1;
    pub const MAX_PRECISION: u8 = 38;

    pub fn new(precision: u8, scale: u8) -> Result<Self, Error> {
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return Err(Error::DecimalPrecision(precision));
        }
        if scale > precision {
            return Err(Error::DecimalScale { precision, scale });
        }

        Ok(Decimal { precision, scale })
    }

    pub const fn precision(self) -> u8 {
        self.precision
    }

    pub const fn scale(self) -> u8 {
        self.scale
    }
}

impl Default for Decimal {
    fn default() -> Self {
        Decimal {
            precision: 10,
            scale: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds() {
        assert_eq!(Decimal::new(0, 0), Err(Error::DecimalPrecision(0)));
        assert_eq!(Decimal::new(39, 0), Err(Error::DecimalPrecision(39)));
        assert_eq!(
            Decimal::new(5, 6),
            Err(Error::DecimalScale {
                precision: 5,
                scale: 6
            })
        );
        assert!(Decimal::new(38, 38).is_ok());
        assert!(Decimal::new(1, 0).is_ok());
    }

    #[test]
    fn default_is_sql_default() {
        let d = Decimal::default();
        assert_eq!((d.precision(), d.scale()), (10, 0));
    }
}
