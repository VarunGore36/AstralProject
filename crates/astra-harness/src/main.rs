use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(name = "astra-harness", version)]
#[command(about = "Run a benchmark: one capture plus one config into a hashed report")]
struct Cli {
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
    #[arg(long, value_name = "FILE")]
    config: PathBuf,
    #[arg(long, value_name = "FILE")]
    output: PathBuf,
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
    let config = std::fs::read_to_string(&cli.config).map_err(|error| error.to_string())?;
    let run =
        astra_harness::run_benchmark(&cli.input, &config).map_err(|error| error.to_string())?;
    std::fs::write(&cli.output, &run.bytes).map_err(|error| error.to_string())?;

    println!("input       {}", cli.input.display());
    println!("config      {}", cli.config.display());
    println!("probes      {}", run.probes);
    println!("fills       {}", run.fills);
    println!("report      {}", cli.output.display());
    println!("report_hash {}", run.hash);

    Ok(())
}
