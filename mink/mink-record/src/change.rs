//! The change type of a row: append, insert, update before and after, or delete, with its byte code.

use std::fmt;

use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ChangeType {
    AppendOnly = 0,
    Insert = 1,
    UpdateBefore = 2,
    UpdateAfter = 3,
    Delete = 4,
}

impl ChangeType {
    pub const ALL: [ChangeType; 5] = [
        ChangeType::AppendOnly,
        ChangeType::Insert,
        ChangeType::UpdateBefore,
        ChangeType::UpdateAfter,
        ChangeType::Delete,
    ];

    pub fn from_byte(byte: u8) -> Result<Self, Error> {
        ChangeType::ALL
            .into_iter()
            .find(|change| change.byte() == byte)
            .ok_or(Error::ChangeType(byte))
    }

    pub fn byte(self) -> u8 {
        self as u8
    }

    pub const fn short(self) -> &'static str {
        match self {
            ChangeType::AppendOnly => "+A",
            ChangeType::Insert => "+I",
            ChangeType::UpdateBefore => "-U",
            ChangeType::UpdateAfter => "+U",
            ChangeType::Delete => "-D",
        }
    }

    pub fn is_add(self) -> bool {
        matches!(
            self,
            ChangeType::AppendOnly | ChangeType::Insert | ChangeType::UpdateAfter
        )
    }
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_round_trip() {
        let expected = [
            (ChangeType::AppendOnly, 0, "+A"),
            (ChangeType::Insert, 1, "+I"),
            (ChangeType::UpdateBefore, 2, "-U"),
            (ChangeType::UpdateAfter, 3, "+U"),
            (ChangeType::Delete, 4, "-D"),
        ];
        for (change, byte, short) in expected {
            assert_eq!(change.byte(), byte);
            assert_eq!(ChangeType::from_byte(byte).unwrap(), change);
            assert_eq!(change.to_string(), short);
        }
        assert_eq!(ChangeType::from_byte(5).unwrap_err(), Error::ChangeType(5));
    }

    #[test]
    fn retractions_are_not_adds() {
        assert!(ChangeType::Insert.is_add());
        assert!(ChangeType::UpdateAfter.is_add());
        assert!(!ChangeType::UpdateBefore.is_add());
        assert!(!ChangeType::Delete.is_add());
    }
}
