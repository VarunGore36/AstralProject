use std::path::PathBuf;
use std::process::ExitCode;

use astra_replay::{BookTop, TradeTally, replay};
use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(name = "astra-replay", version)]
#[command(about = "Deterministically replay captures through strategies")]
struct Cli {
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
    #[arg(long, value_name = "SEED", default_value_t = 7)]
    seed: u64,
    #[arg(long, value_enum)]
    strategy: StrategyName,
}

#[derive(Clone, Copy, ValueEnum)]
enum StrategyName {
    BookTop,
    TradeTally,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("astra-replay: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), astra_replay::ReplayError> {
    match cli.strategy {
        StrategyName::BookTop => run_with(BookTop::new(), &cli),
        StrategyName::TradeTally => run_with(TradeTally::new(), &cli),
    }
}

fn run_with(
    strategy: impl astra_replay::Strategy,
    cli: &Cli,
) -> Result<(), astra_replay::ReplayError> {
    let mut strategy = strategy;
    let report = replay(&cli.input, cli.seed, &mut strategy)?;

    print!(
        "{}",
        astra_replay::format_report(&cli.input, cli.seed, &report)
    );

    Ok(())
}
