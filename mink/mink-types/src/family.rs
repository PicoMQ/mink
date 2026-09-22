//! Type families that group roots by their SQL semantics.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    Predefined,
    Constructed,
    CharacterString,
    BinaryString,
    Numeric,
    IntegerNumeric,
    ExactNumeric,
    ApproximateNumeric,
    Datetime,
    Time,
    Timestamp,
    Collection,
    Extension,
}
