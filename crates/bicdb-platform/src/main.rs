use clap::Parser;

fn main() -> anyhow::Result<()> {
    bicdb_platform::cli::run(bicdb_platform::cli::Cli::parse())
}
