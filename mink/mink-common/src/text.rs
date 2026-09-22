//! Plain-text layout of tabular output: aligned columns and aligned key-value pairs.

pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
    }

    let line = |cells: Vec<&str>| {
        let mut out = String::new();
        for (i, cell) in cells.iter().enumerate() {
            if i > 0 {
                out.push_str("  ");
            }
            if i + 1 == cells.len() {
                out.push_str(cell);
            } else {
                out.push_str(&format!("{cell:<width$}", width = widths[i]));
            }
        }
        out.trim_end().to_owned()
    };

    let mut out = line(headers.to_vec());
    for row in rows {
        out.push('\n');
        out.push_str(&line(row.iter().map(String::as_str).collect()));
    }

    out
}

pub fn pairs(pairs: &[(&str, String)]) -> String {
    let width = pairs.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
    pairs
        .iter()
        .map(|(k, v)| format!("{k:<width$}  {v}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_pads_every_column_but_the_last() {
        let text = table(
            &["id", "name", "note"],
            &[
                vec!["1".into(), "alpha".into(), "x".into()],
                vec!["10".into(), "b".into(), "".into()],
            ],
        );
        assert_eq!(text, "id  name   note\n1   alpha  x\n10  b");
    }

    #[test]
    fn pairs_align_values() {
        assert_eq!(
            pairs(&[("a", "1".into()), ("long", "2".into())]),
            "a     1\nlong  2"
        );
    }
}
