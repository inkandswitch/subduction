//! Release tooling for the Subduction workspace.
//!
//! Run as `cargo xtask <command>` (an alias in `.cargo/config.toml`). Release
//! logic lives here instead of in workflow YAML, so each CI step runs one command.
//!
//! The npm packages have their own tool, `scripts/js-release.py`; this one
//! covers the crates published to crates.io.

mod crates;

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Release the workspace crates to crates.io.
    #[command(subcommand)]
    Crates(crates::Command),
}

fn main() -> ExitCode {
    let Cli { command } = Cli::parse();
    let result = match command {
        Command::Crates(command) => crates::run(command),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}
