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
    let summary = astra_record::reconstruct::reconstruct(&args.input, args.snapshot.as_deref())?;

    println!("input       {}", args.input.display());
    println!(
        "snapshot    {}",
        args.snapshot
            .as_deref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "none (book is partial)".to_owned())
    );
    println!("records     {}", summary.records);
    println!("venue       {}", summary.venue_frames);
    println!("synthetic   {}", summary.synthetic_records);
    println!("applied     {}", summary.diffs_applied);
    println!("unchecked   {}", summary.frames_without_a_book);
    println!("invalid     {}", summary.invalid_diffs);
    println!("skipped     {}", summary.skipped_before_snapshot);
    println!("gaps        {}", summary.gaps);
    println!("rejected    {}", summary.rejected_after_gap);
    println!("bid levels  {}", summary.book.bids_len());
    println!("ask levels  {}", summary.book.asks_len());
    println!("best bid    {}", describe_level(summary.book.best_bid()));
    println!("best ask    {}", describe_level(summary.book.best_ask()));
    println!("mid         {}", describe_fixed(summary.book.mid()));
    println!("spread      {}", describe_fixed(summary.book.spread()));
    println!("crossed     {}", summary.book.is_crossed());

    if summary.snapshot_loaded.is_none() {
        println!("note        the book is partial: no snapshot bootstrap");
    }
    if let Some(error) = summary.first_error {
        println!("first error {error}");
    }

    Ok(())
}

fn verify(args: VerifyArgs) -> Result<(), RecordError> {
    let report = astra_record::compare::compare(&args.input, &args.reference, args.levels)?;

    println!("input       {}", args.input.display());
    println!("reference   {}", args.reference.display());
    println!("levels      {}", args.levels);
    println!("events      {}", report.events);
    println!("applied     {}", report.events_applied);
    println!("skipped     {}", report.events_skipped);
    println!("rejected    {}", report.events_rejected);
    println!("bootstrap   {}", report.bootstrap_frames);
    println!("checks      {}", report.checked);
    println!("matched     {}", report.matched);
    println!("mismatched  {}", report.mismatched);

    if let Some(mismatch) = report.first_mismatch {
        println!("first       {mismatch}");
    }

    if report.checked == 0 {
        println!("note        nothing was checked, so nothing is verified");
    }

    Ok(())
}

fn check_capture(args: CheckArgs) -> Result<(), RecordError> {
    let report = astra_record::check::check(&args.input)?;

    println!("input       {}", args.input.display());
    println!("chunks      {}", report.chunks);
    println!("records     {}", report.records);
    println!("venue       {}", report.venue_frames);
    println!("synthetic   {}", report.synthetic_records);
    println!("checked     {}", report.checked_frames);
    println!("unchecked   {}", report.unchecked_frames);
    println!("conn_gaps   {}", report.connection_gaps);
    println!("seq_gaps    {}", report.update_id_gaps.len());
    println!("seq_breaks  {}", report.seq_breaks.len());
    println!(
        "manifest    {}",
        report
            .manifest_frames
            .map(|frames| frames.to_string())
            .unwrap_or_else(|| "missing".to_owned())
    );
    println!(
        "span        {}",
        match (report.first_ts, report.last_ts) {
            (Some(first), Some(last)) => format!(
                "{}s first to last",
                last.unix_nanos().saturating_sub(first.unix_nanos()) as f64 / 1_000_000_000.0
            ),
            _ => "empty".to_owned(),
        }
    );

    for gap in &report.gap_details {
        println!(
            "gap         seq {} attempts {} {}",
            gap.seq, gap.attempts, gap.reason
        );
    }
    for id_gap in &report.update_id_gaps {
        if id_gap.expected != 0 {
            println!(
                "update_gap  expected {} saw {}",
                id_gap.expected, id_gap.found
            );
        }
    }
    for seq_break in &report.seq_breaks {
        println!(
            "seq_break   expected {} found {}",
            seq_break.expected, seq_break.found
        );
    }

    println!(
        "verdict     {}",
        if report.is_healthy() {
            "healthy"
        } else {
            "issues found, see above"
        }
    );

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
