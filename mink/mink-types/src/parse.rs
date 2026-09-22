//! Tokenizer and recursive-descent parser turning the SQL-style textual form back into a data type.

use std::fmt;
use std::iter::Peekable;
use std::str::{CharIndices, FromStr};

use crate::{DataType, Decimal, Error, Field, Fields, Length, Precision};

impl FromStr for DataType {
    type Err = Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut parser = Parser {
            input,
            tokens: tokenize(input)?,
            pos: 0,
        };
        let data_type = parser.type_with_nullability()?;

        match parser.tokens.get(parser.pos) {
            Some((at, token)) => Err(parse_error(*at, format!("unexpected {token}"))),
            None => Ok(data_type),
        }
    }
}

fn parse_error(position: usize, message: impl Into<String>) -> Error {
    Error::Parse {
        position,
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Lt,
    Gt,
    LParen,
    RParen,
    Comma,
    Int(u64),
    Str(String),
    Ident(String),
    Keyword(Keyword),
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Lt => f.write_str("`<`"),
            Token::Gt => f.write_str("`>`"),
            Token::LParen => f.write_str("`(`"),
            Token::RParen => f.write_str("`)`"),
            Token::Comma => f.write_str("`,`"),
            Token::Int(value) => write!(f, "{value}"),
            Token::Str(text) => write!(f, "'{text}'"),
            Token::Ident(name) => write!(f, "`{name}`"),
            Token::Keyword(keyword) => write!(f, "{keyword}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keyword {
    Char,
    String,
    Boolean,
    Binary,
    Bytes,
    Decimal,
    Numeric,
    Dec,
    TinyInt,
    SmallInt,
    Int,
    Integer,
    BigInt,
    Float,
    Double,
    Precision,
    Date,
    Time,
    With,
    Without,
    Local,
    Zone,
    Timestamp,
    TimestampLtz,
    Array,
    Map,
    Row,
    Not,
    Null,
}

const KEYWORDS: &[(&str, Keyword)] = &[
    ("CHAR", Keyword::Char),
    ("STRING", Keyword::String),
    ("BOOLEAN", Keyword::Boolean),
    ("BINARY", Keyword::Binary),
    ("BYTES", Keyword::Bytes),
    ("DECIMAL", Keyword::Decimal),
    ("NUMERIC", Keyword::Numeric),
    ("DEC", Keyword::Dec),
    ("TINYINT", Keyword::TinyInt),
    ("SMALLINT", Keyword::SmallInt),
    ("INT", Keyword::Int),
    ("INTEGER", Keyword::Integer),
    ("BIGINT", Keyword::BigInt),
    ("FLOAT", Keyword::Float),
    ("DOUBLE", Keyword::Double),
    ("PRECISION", Keyword::Precision),
    ("DATE", Keyword::Date),
    ("TIME", Keyword::Time),
    ("WITH", Keyword::With),
    ("WITHOUT", Keyword::Without),
    ("LOCAL", Keyword::Local),
    ("ZONE", Keyword::Zone),
    ("TIMESTAMP", Keyword::Timestamp),
    ("TIMESTAMP_LTZ", Keyword::TimestampLtz),
    ("ARRAY", Keyword::Array),
    ("MAP", Keyword::Map),
    ("ROW", Keyword::Row),
    ("NOT", Keyword::Not),
    ("NULL", Keyword::Null),
];

// Reserved so a future type does not silently parse as a field name.
const UNSUPPORTED: &[&str] = &[
    "VARCHAR",
    "VARBINARY",
    "INTERVAL",
    "MULTISET",
    "RAW",
    "LEGACY",
    "VARIANT",
    "BITMAP",
];

impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (name, _) = KEYWORDS
            .iter()
            .find(|(_, keyword)| keyword == self)
            .expect("every keyword is in the table");
        f.write_str(name)
    }
}

fn is_delimiter(c: char) -> bool {
    c.is_whitespace() || matches!(c, '<' | '>' | '(' | ')' | ',' | '.')
}

fn tokenize(input: &str) -> Result<Vec<(usize, Token)>, Error> {
    let mut tokens = Vec::new();
    let mut chars = input.char_indices().peekable();
    while let Some((at, c)) = chars.next() {
        let token = match c {
            '<' => Token::Lt,
            '>' => Token::Gt,
            '(' => Token::LParen,
            ')' => Token::RParen,
            ',' => Token::Comma,
            '\'' => Token::Str(quoted(&mut chars, at, '\'')?),
            '`' => Token::Ident(quoted(&mut chars, at, '`')?),
            c if c.is_whitespace() => continue,
            c if c.is_ascii_digit() => {
                let end = run(input, at, |c| c.is_ascii_digit());
                advance(&mut chars, end);
                let value = input[at..end]
                    .parse()
                    .map_err(|_| parse_error(at, "integer out of range"))?;
                Token::Int(value)
            }
            c if is_delimiter(c) => {
                return Err(parse_error(at, format!("unexpected character `{c}`")));
            }
            _ => {
                let end = run(input, at, |c| !is_delimiter(c));
                advance(&mut chars, end);
                word(&input[at..end], at)?
            }
        };
        tokens.push((at, token));
    }

    Ok(tokens)
}

fn run(input: &str, start: usize, keep: impl Fn(char) -> bool) -> usize {
    input[start..]
        .char_indices()
        .find(|(_, c)| !keep(*c))
        .map_or(input.len(), |(i, _)| start + i)
}

fn advance(chars: &mut Peekable<CharIndices<'_>>, end: usize) {
    while chars.peek().is_some_and(|(i, _)| *i < end) {
        chars.next();
    }
}

fn quoted(
    chars: &mut Peekable<CharIndices<'_>>,
    start: usize,
    delimiter: char,
) -> Result<String, Error> {
    let mut text = String::new();
    loop {
        match chars.next() {
            None => return Err(parse_error(start, format!("unterminated {delimiter}"))),
            Some((_, c)) if c == delimiter => {
                if chars.peek().is_some_and(|(_, next)| *next == delimiter) {
                    chars.next();
                    text.push(c);
                } else {
                    return Ok(text);
                }
            }
            Some((_, c)) => text.push(c),
        }
    }
}

fn word(text: &str, at: usize) -> Result<Token, Error> {
    let upper = text.to_ascii_uppercase();
    if UNSUPPORTED.contains(&upper.as_str()) {
        return Err(parse_error(at, format!("unsupported keyword {upper}")));
    }

    Ok(KEYWORDS
        .iter()
        .find(|(name, _)| *name == upper)
        .map_or_else(
            || Token::Ident(text.to_owned()),
            |(_, keyword)| Token::Keyword(*keyword),
        ))
}

struct Parser<'a> {
    input: &'a str,
    tokens: Vec<(usize, Token)>,
    pos: usize,
}

impl Parser<'_> {
    fn position(&self) -> usize {
        self.tokens
            .get(self.pos)
            .map_or(self.input.len(), |(at, _)| *at)
    }

    fn peek(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset).map(|(_, token)| token)
    }

    fn peek_keyword(&self, offset: usize, keyword: Keyword) -> bool {
        matches!(self.peek(offset), Some(Token::Keyword(k)) if *k == keyword)
    }

    fn next(&mut self) -> Result<Token, Error> {
        let (_, token) = self
            .tokens
            .get(self.pos)
            .ok_or_else(|| parse_error(self.input.len(), "unexpected end"))?;
        self.pos += 1;
        Ok(token.clone())
    }

    fn expect(&mut self, expected: Token) -> Result<(), Error> {
        let at = self.position();
        let found = self.next()?;
        if found == expected {
            Ok(())
        } else {
            Err(parse_error(
                at,
                format!("expected {expected}, found {found}"),
            ))
        }
    }

    fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), Error> {
        self.expect(Token::Keyword(keyword))
    }

    fn expect_int<T: TryFrom<u64>>(&mut self) -> Result<T, Error> {
        let at = self.position();
        match self.next()? {
            Token::Int(value) => {
                T::try_from(value).map_err(|_| parse_error(at, "integer out of range"))
            }
            found => Err(parse_error(
                at,
                format!("expected an integer, found {found}"),
            )),
        }
    }

    fn expect_ident(&mut self) -> Result<String, Error> {
        let at = self.position();
        match self.next()? {
            Token::Ident(name) => Ok(name),
            found => Err(parse_error(
                at,
                format!("expected a field name, found {found}"),
            )),
        }
    }

    fn param<T>(at: usize, result: Result<T, Error>) -> Result<T, Error> {
        result.map_err(|e| parse_error(at, e.to_string()))
    }

    fn type_with_nullability(&mut self) -> Result<DataType, Error> {
        let base = self.type_by_keyword()?;
        let data_type = base.with_nullable(self.nullability());
        if self.peek_keyword(0, Keyword::Array) {
            self.pos += 1;
            let nullable = self.nullability();
            return Ok(DataType::array(data_type).with_nullable(nullable));
        }

        Ok(data_type)
    }

    fn nullability(&mut self) -> bool {
        if self.peek_keyword(0, Keyword::Not) && self.peek_keyword(1, Keyword::Null) {
            self.pos += 2;
            return false;
        }

        if self.peek_keyword(0, Keyword::Null) {
            self.pos += 1;
        }
        true
    }

    fn type_by_keyword(&mut self) -> Result<DataType, Error> {
        let at = self.position();
        let keyword = match self.next()? {
            Token::Keyword(keyword) => keyword,
            found => return Err(parse_error(at, format!("expected a type, found {found}"))),
        };
        match keyword {
            Keyword::Boolean => Ok(DataType::boolean()),
            Keyword::TinyInt => Ok(DataType::tiny_int()),
            Keyword::SmallInt => Ok(DataType::small_int()),
            Keyword::Int | Keyword::Integer => Ok(DataType::int()),
            Keyword::BigInt => Ok(DataType::big_int()),
            Keyword::Float => Ok(DataType::float()),
            Keyword::Double => {
                if self.peek_keyword(0, Keyword::Precision) {
                    self.pos += 1;
                }
                Ok(DataType::double())
            }
            Keyword::Char => Ok(DataType::char(self.optional_length()?)),
            Keyword::String => Ok(DataType::string()),
            Keyword::Binary => Ok(DataType::binary(self.optional_length()?)),
            Keyword::Bytes => Ok(DataType::bytes()),
            Keyword::Decimal | Keyword::Numeric | Keyword::Dec => {
                Ok(DataType::decimal(self.decimal()?))
            }
            Keyword::Date => Ok(DataType::date()),
            Keyword::Time => {
                let precision = self.optional_precision(Precision::SECONDS)?;
                if self.peek_keyword(0, Keyword::Without) {
                    self.pos += 1;
                    self.expect_keyword(Keyword::Time)?;
                    self.expect_keyword(Keyword::Zone)?;
                }
                Ok(DataType::time(precision))
            }
            Keyword::Timestamp => {
                let precision = self.optional_precision(Precision::MICROS)?;
                if self.peek_keyword(0, Keyword::Without) {
                    self.pos += 1;
                    self.expect_keyword(Keyword::Time)?;
                    self.expect_keyword(Keyword::Zone)?;
                    Ok(DataType::timestamp(precision))
                } else if self.peek_keyword(0, Keyword::With) {
                    let at = self.position();
                    self.pos += 1;
                    if !self.peek_keyword(0, Keyword::Local) {
                        return Err(parse_error(at, "timestamp with time zone is not supported"));
                    }
                    self.pos += 1;
                    self.expect_keyword(Keyword::Time)?;
                    self.expect_keyword(Keyword::Zone)?;
                    Ok(DataType::timestamp_ltz(precision))
                } else {
                    Ok(DataType::timestamp(precision))
                }
            }
            Keyword::TimestampLtz => Ok(DataType::timestamp_ltz(
                self.optional_precision(Precision::MICROS)?,
            )),
            Keyword::Array => {
                self.expect(Token::Lt)?;
                let element = self.type_with_nullability()?;
                self.expect(Token::Gt)?;
                Ok(DataType::array(element))
            }
            Keyword::Map => {
                self.expect(Token::Lt)?;
                let key = self.type_with_nullability()?;
                self.expect(Token::Comma)?;
                let value = self.type_with_nullability()?;
                self.expect(Token::Gt)?;
                Ok(DataType::map(key, value))
            }
            Keyword::Row => self.row(),
            Keyword::Precision
            | Keyword::With
            | Keyword::Without
            | Keyword::Local
            | Keyword::Zone
            | Keyword::Not
            | Keyword::Null => Err(parse_error(at, format!("expected a type, found {keyword}"))),
        }
    }

    fn optional_length(&mut self) -> Result<Length, Error> {
        if self.peek(0) != Some(&Token::LParen) {
            return Ok(Length::default());
        }
        self.pos += 1;
        let at = self.position();
        let length = self.expect_int()?;
        self.expect(Token::RParen)?;
        Self::param(at, Length::new(length))
    }

    fn optional_precision(&mut self, default: Precision) -> Result<Precision, Error> {
        if self.peek(0) != Some(&Token::LParen) {
            return Ok(default);
        }
        self.pos += 1;
        let at = self.position();
        let precision = self.expect_int()?;
        self.expect(Token::RParen)?;
        Self::param(at, Precision::new(precision))
    }

    fn decimal(&mut self) -> Result<Decimal, Error> {
        if self.peek(0) != Some(&Token::LParen) {
            return Ok(Decimal::default());
        }
        self.pos += 1;
        let at = self.position();
        let precision = self.expect_int()?;
        let scale = if self.peek(0) == Some(&Token::Comma) {
            self.pos += 1;
            self.expect_int()?
        } else {
            0
        };
        self.expect(Token::RParen)?;
        Self::param(at, Decimal::new(precision, scale))
    }

    fn row(&mut self) -> Result<DataType, Error> {
        let at = self.position();
        let close = match self.next()? {
            Token::Lt => Token::Gt,
            Token::LParen => Token::RParen,
            found => {
                return Err(parse_error(
                    at,
                    format!("expected `<` or `(`, found {found}"),
                ));
            }
        };
        let mut fields = Vec::new();
        while self.peek(0) != Some(&close) {
            if !fields.is_empty() {
                self.expect(Token::Comma)?;
            }
            let at = self.position();
            let name = self.expect_ident()?;
            let data_type = self.type_with_nullability()?;
            let mut field = Self::param(at, Field::new(name, data_type))?;
            if let Some(Token::Str(_)) = self.peek(0) {
                let Token::Str(description) = self.next()? else {
                    unreachable!("peeked a string");
                };
                field = field.with_description(description);
            }
            fields.push(field);
        }
        let at = self.position();
        self.pos += 1;
        Ok(DataType::row(Self::param(at, Fields::new(fields))?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Kind;

    fn parse(input: &str) -> DataType {
        input.parse().unwrap_or_else(|e| panic!("{input}: {e}"))
    }

    fn position_of(input: &str) -> usize {
        match input.parse::<DataType>() {
            Err(Error::Parse { position, .. }) => position,
            other => panic!("{input}: expected a parse error, got {other:?}"),
        }
    }

    #[test]
    fn leaves_and_synonyms() {
        assert_eq!(parse("INT"), DataType::int());
        assert_eq!(parse("integer"), DataType::int());
        assert_eq!(parse("DOUBLE PRECISION"), DataType::double());
        assert_eq!(parse("CHAR"), DataType::char(Length::default()));
        assert_eq!(
            parse("BINARY(16)"),
            DataType::binary(Length::new(16).unwrap())
        );
        assert_eq!(parse("DECIMAL"), DataType::decimal(Decimal::default()));
        assert_eq!(
            parse("NUMERIC(5)"),
            DataType::decimal(Decimal::new(5, 0).unwrap())
        );
        assert_eq!(
            parse("DEC(5, 2)"),
            DataType::decimal(Decimal::new(5, 2).unwrap())
        );
    }

    #[test]
    fn temporal_forms() {
        assert_eq!(parse("TIME"), DataType::time(Precision::SECONDS));
        assert_eq!(
            parse("TIME(3) WITHOUT TIME ZONE"),
            DataType::time(Precision::MILLIS)
        );
        assert_eq!(parse("TIMESTAMP"), DataType::timestamp(Precision::MICROS));
        assert_eq!(
            parse("TIMESTAMP(9) WITHOUT TIME ZONE"),
            DataType::timestamp(Precision::NANOS)
        );
        assert_eq!(
            parse("TIMESTAMP(3) WITH LOCAL TIME ZONE"),
            DataType::timestamp_ltz(Precision::MILLIS)
        );
        assert_eq!(
            parse("TIMESTAMP_LTZ"),
            DataType::timestamp_ltz(Precision::MICROS)
        );
        assert_eq!(position_of("TIMESTAMP WITH TIME ZONE"), 10);
    }

    #[test]
    fn nullability() {
        assert!(!parse("INT NOT NULL").is_nullable());
        assert!(parse("INT NULL").is_nullable());
        let array = parse("INT NOT NULL ARRAY NOT NULL");
        assert!(!array.is_nullable());
        assert!(!array.children()[0].is_nullable());
        let array = parse("ARRAY<INT NOT NULL>");
        assert!(array.is_nullable());
        assert!(!array.children()[0].is_nullable());
    }

    #[test]
    fn map_key_becomes_non_nullable() {
        let map = parse("MAP<STRING, INT>");
        let Kind::Map { key, value } = map.kind() else {
            panic!("expected map");
        };
        assert!(!key.is_nullable());
        assert!(value.is_nullable());
    }

    #[test]
    fn rows_in_both_syntaxes_with_descriptions_and_escapes() {
        let expected = DataType::row(
            Fields::new(vec![
                Field::new("id", DataType::big_int().with_nullable(false)).unwrap(),
                Field::new("tag`s", DataType::array(DataType::string()))
                    .unwrap()
                    .with_description("it's tags"),
            ])
            .unwrap(),
        );
        assert_eq!(
            parse("ROW<`id` BIGINT NOT NULL, `tag``s` ARRAY<STRING> 'it''s tags'>"),
            expected
        );
        assert_eq!(
            parse("row(id bigint not null, `tag``s` string array 'it''s tags')"),
            expected
        );
        assert_eq!(parse("ROW<>"), DataType::row(Fields::new(vec![]).unwrap()));
    }

    #[test]
    fn keyword_field_names_must_be_quoted() {
        assert_eq!(position_of("ROW<date INT>"), 4);
        assert!(parse("ROW<`date` INT>").is(crate::Family::Constructed));
    }

    #[test]
    fn errors_carry_positions() {
        assert_eq!(position_of(""), 0);
        assert_eq!(position_of("INT INT"), 4);
        assert_eq!(position_of("ARRAY<INT"), 9);
        assert_eq!(position_of("VARCHAR(10)"), 0);
        assert_eq!(position_of("MAP<INT INT>"), 8);
        assert_eq!(position_of("ROW<a INT, a INT>"), 16);
        assert_eq!(position_of("ROW<`` INT>"), 4);
        assert_eq!(position_of("'abc"), 0);
        assert_eq!(position_of("a.b"), 1);
        assert_eq!(position_of("CHAR(0)"), 5);
        assert_eq!(position_of("DECIMAL(40)"), 8);
        assert_eq!(position_of("DECIMAL(5, 6)"), 8);
        assert_eq!(position_of("TIME(10)"), 5);
        assert_eq!(position_of("CHAR(99999999999999999999)"), 5);
        assert_eq!(position_of("CHAR(x)"), 5);
    }
}
