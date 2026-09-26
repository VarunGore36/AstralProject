use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use astra_record::capture::{CaptureOptions, MANIFEST_FILE, init_capture, run_capture};
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
    #[arg(long, value_name = "SECONDS")]
    duration_secs: Option<u64>,
    #[arg(long, value_name = "FRAMES")]
    max_frames: Option<u64>,
    #[arg(long, value_name = "URL")]
    url: Option<String>,
    #[arg(long, value_name = "COUNT", default_value_t = 5)]
    max_reconnects: u32,
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
    }
}

fn init(args: InitArgs) -> Result<(), RecordError> {
    let instrument = Instrument::new(args.venue, args.market, args.symbol);
    let manifest = init_capture(&args.output, &instrument, args.channel)?;

    println!("capture_id  {}", manifest.capture_id.as_str());
    println!("output      {}", args.output.display());
    println!("instrument  {instrument}");
    println!("channel     {}", args.channel);
    println!("frames      {}", manifest.frames_written);
    println!("manifest    {}", args.output.join(MANIFEST_FILE).display());

    Ok(())
}

fn capture(args: CaptureArgs) -> Result<(), RecordError> {
    let instrument = Instrument::new(args.venue, args.market, args.symbol);
    let url = match args.url {
        Some(url) => url,
        None => feed::stream_url(&instrument, args.channel)?,
    };

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
    };

    let outcome = run_capture(options, interrupted)?;

    println!("capture_id  {}", outcome.capture_id);
    println!("output      {}", args.output.display());
    println!("url         {url}");
    println!("instrument  {instrument}");
    println!("channel     {}", args.channel);
    println!("frames      {}", outcome.frames_written);
    println!("gaps        {}", outcome.gaps_recorded);
    println!("stop_reason {}", outcome.stop_reason);
    println!("manifest    {}", args.output.join(MANIFEST_FILE).display());

    Ok(())
}
