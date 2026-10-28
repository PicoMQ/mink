use anyhow::Context;

pub fn generate(bless: bool) -> anyhow::Result<()> {
    let fixtures = mink_conformance::data_fixtures();
    let written = fixtures
        .iter()
        .map(|fixture| {
            mink_iceberg::build_file(
                fixture.file(),
                fixture.records(),
                fixture.granularity(),
                &mink_iceberg::FileSchema::Binary,
            )
            .with_context(|| fixture.name().to_owned())
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    if !bless {
        println!(
            "wrote {} files in memory; rerun with --bless to write conformance/fixtures/data",
            written.len()
        );
        return Ok(());
    }

    let entries: Vec<_> = fixtures
        .iter()
        .zip(&written)
        .map(|(fixture, (bytes, stats))| (fixture, bytes, stats))
        .collect();
    mink_conformance::bless_data(&entries)?;
    println!("wrote {} files to conformance/fixtures/data", entries.len());

    Ok(())
}
