use clap::Parser;

fn main() -> anyhow::Result<()> {
    bicdb_app_host::cli::run(bicdb_app_host::cli::Cli::parse())
}
