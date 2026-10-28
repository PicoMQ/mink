mod generate;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "mink-conformance", about = "Generate conformance fixtures")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(
        subcommand,
        about = "write conformance fixtures: record batches captured from Kafka, and the files and metadata Mink writes from them"
    )]
    Generate(Fixtures),
}

#[derive(Subcommand)]
enum Fixtures {
    #[command(
        about = "record batches captured from Kafka, with what a read_committed consumer returns"
    )]
    Records {
        #[arg(long, default_value = "localhost:29092")]
        bootstrap: String,
        #[arg(
            long,
            help = "the image of the Kafka broker being captured, written into the manifest"
        )]
        kafka_image: String,
        #[arg(
            long,
            help = "overwrite conformance/fixtures/records; without it nothing is written"
        )]
        bless: bool,
    },
    #[command(about = "version 1 Parquet files of the committed records of every record fixture")]
    Data {
        #[arg(
            long,
            help = "overwrite conformance/fixtures/data; without it nothing is written"
        )]
        bless: bool,
    },
    #[command(about = "the table metadata, manifest lists and manifests of a scripted topic")]
    Metadata {
        #[arg(
            long,
            help = "overwrite conformance/fixtures/metadata; without it nothing is written"
        )]
        bless: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Generate(Fixtures::Records {
            bootstrap,
            kafka_image,
            bless,
        }) => generate::records::generate(&bootstrap, &kafka_image, bless).await,
        Command::Generate(Fixtures::Data { bless }) => generate::data::generate(bless),
        Command::Generate(Fixtures::Metadata { bless }) => {
            generate::metadata::generate(bless).await
        }
    }
}
