use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "astra-harness", version)]
#[command(about = "Run benchmarks, keep a registry, reproduce results")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run one capture plus one config into a hashed report.
    Run(RunArgs),
    /// List every recorded run in a registry.
    List(ListArgs),
    /// Reproduce a recorded run against a capture directory.
    Verify(VerifyArgs),
}

#[derive(clap::Args)]
struct RunArgs {
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
    #[arg(long, value_name = "FILE")]
    config: PathBuf,
    #[arg(long, value_name = "FILE")]
    output: PathBuf,
    /// Also record the run (report plus config) into this registry.
    #[arg(long, value_name = "DIR")]
    record: Option<PathBuf>,
}

#[derive(clap::Args)]
struct ListArgs {
    #[arg(long, value_name = "DIR")]
    registry: PathBuf,
}

#[derive(clap::Args)]
struct VerifyArgs {
    #[arg(long, value_name = "DIR")]
    registry: PathBuf,
    #[arg(long, value_name = "HASH")]
    hash: String,
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("astra-harness: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Run(args) => run_benchmark(args),
        Command::List(args) => list_registry(args),
        Command::Verify(args) => verify_run(args),
    }
}

fn run_benchmark(args: RunArgs) -> Result<(), String> {
    let config = std::fs::read_to_string(&args.config).map_err(|error| error.to_string())?;
    let run =
        astra_harness::run_benchmark(&args.input, &config).map_err(|error| error.to_string())?;
    astra_harness::write_report_file(&args.output, &run.bytes)
        .map_err(|error| error.to_string())?;

    print!(
        "{}",
        astra_harness::format_run_report(&args.input, &args.config, &args.output, &run)
    );

    if let Some(registry) = args.record {
        let dir = astra_harness::record_run(&registry, &config, &run)
            .map_err(|error| error.to_string())?;
        println!("recorded    {}", dir.display());
    }

    Ok(())
}

fn list_registry(args: ListArgs) -> Result<(), String> {
    let runs = astra_harness::list_runs(&args.registry).map_err(|error| error.to_string())?;

    print!(
        "{}",
        astra_harness::format_list_report(&args.registry, &runs)
    );

    Ok(())
}

fn verify_run(args: VerifyArgs) -> Result<(), String> {
    let reproduced = astra_harness::verify_run(&args.registry, &args.hash, &args.input)
        .map_err(|error| error.to_string())?;

    print!(
        "{}",
        astra_harness::format_verify_report(&args.registry, &args.hash, &args.input, reproduced)
    );

    // A mismatch is a failed verification, not a successful report about
    // failure: exit non-zero so scripts and CI can judge mechanically,
    // exactly like ops/soak.sh check does for captures.
    if reproduced {
        Ok(())
    } else {
        Err("reproduction mismatch".to_owned())
    }
}
