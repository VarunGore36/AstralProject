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
    Reconstruct(ReconstructArgs),
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

#[derive(clap::Args)]
struct ReconstructArgs {
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
    println!("checked     {}", outcome.checked_frames);
    println!("conn_gaps   {}", outcome.connection_gaps);
    println!("seq_gaps    {}", outcome.sequence_gaps);
    println!("stop_reason {}", outcome.stop_reason);
    println!("manifest    {}", args.output.join(MANIFEST_FILE).display());

    Ok(())
}

fn reconstruct(args: ReconstructArgs) -> Result<(), RecordError> {
    let summary = astra_record::reconstruct::reconstruct(&args.input)?;

    println!("input       {}", args.input.display());
    println!("records     {}", summary.records);
    println!("venue       {}", summary.venue_frames);
    println!("synthetic   {}", summary.synthetic_records);
    println!("applied     {}", summary.diffs_applied);
    println!("unchecked   {}", summary.frames_without_a_book);
    println!("invalid     {}", summary.invalid_diffs);
    println!("bid levels  {}", summary.book.bids_len());
    println!("ask levels  {}", summary.book.asks_len());
    println!("best bid    {}", describe_level(summary.book.best_bid()));
    println!("best ask    {}", describe_level(summary.book.best_ask()));
    println!("mid         {}", describe_fixed(summary.book.mid()));
    println!("spread      {}", describe_fixed(summary.book.spread()));
    println!("crossed     {}", summary.book.is_crossed());
    println!("note        the book is partial: no snapshot bootstrap yet");

    if let Some(error) = summary.first_error {
        println!("first error {error}");
    }

    Ok(())
}

fn describe_level(level: Option<astra_book::Level>) -> String {
    match level {
        Some(level) => format!("{} x {}", level.price, level.quantity),
        None => "none".to_owned(),
    }
}

fn describe_fixed(value: Option<astra_types::Fixed>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "none".to_owned(),
    }
}
