//! Writes a command's result to stdout either as pretty JSON or as the text the command renders.

use std::io;
use std::io::Write;

use serde::Serialize;

pub use mink_common::text::{pairs, table};
pub use mink_common::time::{duration as duration_ms, when};

pub struct Printer {
    json: bool,
}

impl Printer {
    pub fn new(json: bool) -> Self {
        Printer { json }
    }

    pub fn emit<T: Serialize>(
        &self,
        value: &T,
        text: impl FnOnce() -> String,
    ) -> anyhow::Result<()> {
        let mut out = io::stdout().lock();
        if self.json {
            serde_json::to_writer_pretty(&mut out, value)?;
            out.write_all(b"\n")?;
        } else {
            let text = text();
            out.write_all(text.as_bytes())?;
            if !text.ends_with('\n') {
                out.write_all(b"\n")?;
            }
        }

        Ok(())
    }

    pub fn done(&self, text: impl Into<String>) -> anyhow::Result<()> {
        #[derive(Serialize)]
        struct Ok {
            ok: bool,
        }

        let text = text.into();
        self.emit(&Ok { ok: true }, || text)
    }
}

pub fn opt<T: ToString>(value: &Option<T>) -> String {
    value.as_ref().map_or_else(|| "-".to_owned(), T::to_string)
}

pub fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
