use clap::{Parser, Subcommand};

use mesh::error::Result;
use process::apply;

mod process;

/// relayctl cli app
#[derive(Debug, Parser)]
#[clap(name = "relayctl", version)]
pub struct Relayctl {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Parse a YAML file and send to Relay to apply desired state
    Apply {
        /// Path to the config command. Can be a directory of YAML files or a single YAML file.
        #[clap(long, short = 'f')]
        path: std::path::PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Relayctl::parse();

    match args.command {
        Command::Apply { path } => apply(path).await?,
    }

    Ok(())
}
