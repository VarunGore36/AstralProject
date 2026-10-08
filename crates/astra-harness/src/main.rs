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
    std::fs::write(&args.output, &run.bytes).map_err(|error| error.to_string())?;

    println!("input       {}", args.input.display());
    println!("config      {}", args.config.display());
    println!("probes      {}", run.probes);
    println!("fills       {}", run.fills);
    println!("report      {}", args.output.display());
    println!("report_hash {}", run.hash);

    if let Some(registry) = args.record {
        let dir = astra_harness::record_run(&registry, &config, &run)
            .map_err(|error| error.to_string())?;
        println!("recorded    {}", dir.display());
    }

    Ok(())
}

fn list_registry(args: ListArgs) -> Result<(), String> {
    let runs = astra_harness::list_runs(&args.registry).map_err(|error| error.to_string())?;

    println!("registry    {}", args.registry.display());
    println!("runs        {}", runs.len());
    for run in &runs {
        println!(
            "run         {} probes={} fills={} seed={} capture={}",
            run.hash, run.probes, run.fills, run.seed, run.capture_id
        );
    }

    Ok(())
}

fn verify_run(args: VerifyArgs) -> Result<(), String> {
    let reproduced = astra_harness::verify_run(&args.registry, &args.hash, &args.input)
        .map_err(|error| error.to_string())?;

    println!("registry    {}", args.registry.display());
    println!("hash        {}", args.hash);
    println!("input       {}", args.input.display());
    println!(
        "verdict     {}",
        if reproduced {
            "reproduced"
        } else {
            "MISMATCH, see above"
        }
    );

    Ok(())
}
