//! Runs statements against an engine and writes the rows out as a table, JSON lines or CSV.

use std::io::{self, Write};
use std::time::Instant;

use arrow::csv;
use arrow::json::LineDelimitedWriter;
use arrow::util::pretty;
use arrow_array::RecordBatch;
use clap::ValueEnum;
use mink_query::Engine;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Table,
    Json,
    Csv,
}

pub async fn statement(engine: &Engine, sql: &str, format: Format) -> anyhow::Result<()> {
    let started = Instant::now();
    let batches = engine.sql(sql).await?.collect().await?;
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut out = io::stdout().lock();
    match format {
        Format::Table => {
            let text = pretty::pretty_format_batches(&batches)?;
            writeln!(out, "{text}")?;
            writeln!(
                out,
                "{rows} row{} in {:.3}s",
                if rows == 1 { "" } else { "s" },
                started.elapsed().as_secs_f64()
            )?;
        }
        Format::Json => {
            let mut writer = LineDelimitedWriter::new(&mut out);
            writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
            writer.finish()?;
        }
        Format::Csv => {
            let mut writer = csv::Writer::new(&mut out);
            for batch in &batches {
                writer.write(batch)?;
            }
        }
    }

    Ok(())
}

pub fn split(text: &str) -> Vec<String> {
    text.split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}
