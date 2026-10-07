use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use astra_record::capture::{CaptureOptions, init_capture, run_capture};
use astra_record::error::RecordError;
use astra_record::feed;
use astra_types::{Channel, Instrument, MarketType, Symbol, Venue};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "astra-record", version)]
#[command(about = "Lossless market-data capture")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init(InitArgs),
    Capture(CaptureArgs),
    Reconstruct(ReconstructArgs),
    Verify(VerifyArgs),
    Check(CheckArgs),
}

#[derive(clap::Args)]
struct InitArgs {
    #[arg(long, value_name = "DIR")]
    output: PathBuf,
    #[arg(long)]
    venue: Venue,
    #[arg(long)]
    market: MarketType,
    #[arg(long)]
    symbol: Symbol,
    #[arg(long)]
    channel: Channel,
}

#[derive(clap::Args)]
struct CaptureArgs {
    #[arg(long, value_name = "DIR")]
    output: PathBuf,
    #[arg(long)]
    venue: Venue,
    #[arg(long)]
    market: MarketType,
    #[arg(long)]
    symbol: Symbol,
    #[arg(long)]
    channel: Channel,
    #[arg(long, value_name = "CHANNEL")]
    with: Vec<Channel>,
    #[arg(long, value_name = "SECONDS")]
    duration_secs: Option<u64>,
    #[arg(long, value_name = "FRAMES")]
    max_frames: Option<u64>,
    #[arg(long, value_name = "URL")]
    url: Option<String>,
    #[arg(long, value_name = "COUNT", default_value_t = 5)]
    max_reconnects: u32,
}

#[derive(clap::Args)]
struct ReconstructArgs {
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
    #[arg(long, value_name = "FILE")]
    snapshot: Option<PathBuf>,
}

#[derive(clap::Args)]
struct VerifyArgs {
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
    #[arg(long, value_name = "DIR")]
    reference: PathBuf,
    #[arg(long, value_name = "LEVELS", default_value_t = 10)]
    levels: usize,
    #[arg(long, value_name = "FILE")]
    snapshot: Option<PathBuf>,
    #[arg(long)]
    verbose: bool,
}

#[derive(clap::Args)]
struct CheckArgs {
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("astra-record: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), RecordError> {
    match cli.command {
        Command::Init(args) => init(args),
        Command::Capture(args) => capture(args),
        Command::Reconstruct(args) => reconstruct(args),
        Command::Verify(args) => verify(args),
        Command::Check(args) => check_capture(args),
    }
}

fn init(args: InitArgs) -> Result<(), RecordError> {
    let instrument = Instrument::new(args.venue, args.market, args.symbol);
    let manifest = init_capture(&args.output, &instrument, args.channel)?;

    print!(
        "{}",
        astra_record::capture::format_init_report(&args.output, &manifest)
    );

    Ok(())
}

fn capture(args: CaptureArgs) -> Result<(), RecordError> {
    let instrument = Instrument::new(args.venue, args.market, args.symbol);
    let combined = !args.with.is_empty();
    let url = match args.url.clone() {
        Some(url) => url,
        None if combined => {
            let mut channels = vec![args.channel];
            channels.extend(args.with.iter().copied());
            feed::combined_stream_url(&instrument, &channels)?
        }
        None => feed::stream_url(&instrument, args.channel)?,
    };
    if combined {
        let mut channels = vec![args.channel];
        channels.extend(args.with.iter().copied());
        feed::combined_stream_url(&instrument, &channels)?;
    }

    let fanouts = args
        .with
        .iter()
        .map(|channel| astra_record::capture::Fanout {
            dir: args.output.with_file_name(format!(
                "{}-{channel}",
                args.output
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
            )),
            channel: *channel,
        })
        .collect::<Vec<_>>();

    let interrupted = Arc::new(AtomicBool::new(false));
    let handler = Arc::clone(&interrupted);
    ctrlc::set_handler(move || handler.store(true, Ordering::SeqCst))
        .map_err(|error| RecordError::Signal(error.to_string()))?;

    let options = CaptureOptions {
        output: args.output.clone(),
        instrument: instrument.clone(),
        channel: args.channel,
        url: url.clone(),
        max_frames: args.max_frames,
        duration: args.duration_secs.map(Duration::from_secs),
        max_reconnects: args.max_reconnects,
        fanouts,
    };

    let outcome = run_capture(options, interrupted)?;

    print!(
        "{}",
        astra_record::capture::format_outcome(
            &args.output,
            &url,
            &instrument,
            args.channel,
            &outcome
        )
    );

    Ok(())
}

fn reconstruct(args: ReconstructArgs) -> Result<(), RecordError> {
    let summary = astra_record::reconstruct::reconstruct(&args.input, args.snapshot.as_deref())?;

    print!(
        "{}",
        astra_record::reconstruct::format_summary(&args.input, args.snapshot.as_deref(), &summary)
    );

    Ok(())
}

fn verify(args: VerifyArgs) -> Result<(), RecordError> {
    let report = astra_record::compare::compare(
        &args.input,
        &args.reference,
        args.levels,
        args.snapshot.as_deref(),
    )?;

    print!(
        "{}",
        astra_record::compare::format_report(
            &args.input,
            &args.reference,
            args.levels,
            args.verbose,
            &report
        )
    );

    Ok(())
}

fn check_capture(args: CheckArgs) -> Result<(), RecordError> {
    let report = astra_record::check::check(&args.input)?;

    print!(
        "{}",
        astra_record::check::format_report(&args.input, &report)
    );

    Ok(())
}
