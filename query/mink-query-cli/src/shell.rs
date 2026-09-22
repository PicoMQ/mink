//! An interactive shell: line editing with history, statements terminated by `;`.

use mink_query::Engine;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use crate::run::{self, Format};

pub async fn run(engine: &Engine, format: Format) -> anyhow::Result<()> {
    let mut editor = DefaultEditor::new()?;
    let mut pending = String::new();
    loop {
        let prompt = if pending.is_empty() {
            "mink> "
        } else {
            "  -> "
        };
        let line = match editor.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                pending.clear();
                continue;
            }
            Err(ReadlineError::Eof) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        pending.push_str(&line);
        pending.push('\n');
        if !line.trim_end().ends_with(';') {
            continue;
        }
        let text = std::mem::take(&mut pending);
        editor.add_history_entry(text.trim())?;
        for statement in run::split(&text) {
            if let Err(error) = run::statement(engine, &statement, format).await {
                eprintln!("error: {error}");
            }
        }
    }
}
