//! SQL-style textual rendering of data types and fields.

use std::fmt;

use crate::{DataType, Field, Kind};

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            Kind::Char(length) => write!(f, "CHAR({length})")?,
            Kind::Binary(length) => write!(f, "BINARY({length})")?,
            Kind::Decimal(decimal) => {
                write!(f, "DECIMAL({}, {})", decimal.precision(), decimal.scale())?
            }
            Kind::Time(precision) => write!(f, "TIME({precision})")?,
            Kind::Timestamp(precision) => write!(f, "TIMESTAMP({precision})")?,
            Kind::TimestampLtz(precision) => write!(f, "TIMESTAMP_LTZ({precision})")?,
            Kind::Array(element) => write!(f, "ARRAY<{element}>")?,
            Kind::Map { key, value } => write!(f, "MAP<{key}, {value}>")?,
            Kind::Row(fields) => {
                f.write_str("ROW<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{field}")?;
                }
                f.write_str(">")?;
            }
            leaf => f.write_str(leaf.root().keyword())?,
        }

        if !self.is_nullable() {
            f.write_str(" NOT NULL")?;
        }
        Ok(())
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` {}",
            self.name().replace('`', "``"),
            self.data_type()
        )?;
        if let Some(description) = self.description() {
            write!(f, " '{}'", description.replace('\'', "''"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{DataType, Decimal, Field, Fields, Length, Precision};

    #[test]
    fn leaves() {
        assert_eq!(DataType::int().to_string(), "INT");
        assert_eq!(
            DataType::int().with_nullable(false).to_string(),
            "INT NOT NULL"
        );
        assert_eq!(
            DataType::char(Length::new(3).unwrap()).to_string(),
            "CHAR(3)"
        );
        assert_eq!(
            DataType::decimal(Decimal::new(12, 3).unwrap()).to_string(),
            "DECIMAL(12, 3)"
        );
        assert_eq!(DataType::time(Precision::MILLIS).to_string(), "TIME(3)");
        assert_eq!(
            DataType::timestamp_ltz(Precision::MICROS).to_string(),
            "TIMESTAMP_LTZ(6)"
        );
    }

    #[test]
    fn nested() {
        let row = DataType::row(
            Fields::new(vec![
                Field::new("id", DataType::big_int().with_nullable(false)).unwrap(),
                Field::new("tag`s", DataType::array(DataType::string()))
                    .unwrap()
                    .with_description("it's tags"),
            ])
            .unwrap(),
        );
        let map = DataType::map(DataType::string(), row).with_nullable(false);
        assert_eq!(
            map.to_string(),
            "MAP<STRING NOT NULL, ROW<`id` BIGINT NOT NULL, `tag``s` ARRAY<STRING> 'it''s tags'>> NOT NULL"
        );
    }
}
