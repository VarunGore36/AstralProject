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

    println!("capture_id  {}", outcome.capture_id);
    println!("output      {}", args.output.display());
    println!("url         {url}");
    println!("instrument  {instrument}");
    println!("channel     {}", args.channel);
    println!("frames      {}", outcome.frames_written);
    println!("checked     {}", outcome.checked_frames);
    println!("conn_gaps   {}", outcome.connection_gaps);
    println!("seq_gaps    {}", outcome.sequence_gaps);
    for fanout in &outcome.fanouts {
        println!(
            "fanout      {} {} frames={} checked={} gaps={}",
            fanout.dir.display(),
            fanout.channel,
            fanout.frames_written,
            fanout.checked_frames,
            fanout.connection_gaps + fanout.sequence_gaps,
        );
    }
    if outcome.unknown_frames > 0 {
        println!(
            "unknown     {} (first: {})",
            outcome.unknown_frames,
            outcome
                .first_unknown_stream
                .as_deref()
                .unwrap_or("unparseable")
        );
    }
    println!(
        "latency_us  p50 {:.1} p99 {:.1} max {:.1} ({} frames, read to stored)",
        outcome.latency.p50_ns as f64 / 1_000.0,
        outcome.latency.p99_ns as f64 / 1_000.0,
        outcome.latency.max_ns as f64 / 1_000.0,
        outcome.latency.samples
    );
    println!(
        "book_us     p50 {:.1} p99 {:.1} max {:.1} (updates {}, errors {}, read to book-updated)",
        outcome.book_latency.p50_ns as f64 / 1_000.0,
        outcome.book_latency.p99_ns as f64 / 1_000.0,
        outcome.book_latency.max_ns as f64 / 1_000.0,
        outcome.book_updates,
        outcome.book_errors
    );
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
            .unwrap_or_else(|| format!("in-band only ({})", summary.inband_snapshots))
    );
    println!("records     {}", summary.records);
    println!("venue       {}", summary.venue_frames);
    println!("synthetic   {}", summary.synthetic_records);
    println!("applied     {}", summary.diffs_applied);
    println!("unchecked   {}", summary.frames_without_a_book);
    println!("invalid     {}", summary.invalid_diffs);
    println!("skipped     {}", summary.skipped_before_snapshot);
    println!("inband      {}", summary.inband_snapshots);
    println!("gaps        {}", summary.gaps);
    println!("rejected    {}", summary.rejected_after_gap);
    println!("bid levels  {}", summary.book.bids_len());
    println!("ask levels  {}", summary.book.asks_len());
    println!("best bid    {}", describe_level(summary.book.best_bid()));
    println!("best ask    {}", describe_level(summary.book.best_ask()));
    println!("mid         {}", describe_fixed(summary.book.mid()));
    println!("spread      {}", describe_fixed(summary.book.spread()));
    println!("crossed     {}", summary.book.is_crossed());

    if summary.snapshot_loaded.is_none() && summary.inband_snapshots == 0 {
        println!("note        the book is partial: no snapshot bootstrap");
    }
    if let Some(error) = summary.first_error {
        println!("first error {error}");
    }

    Ok(())
}

fn verify(args: VerifyArgs) -> Result<(), RecordError> {
    let report = astra_record::compare::compare(
        &args.input,
        &args.reference,
        args.levels,
        args.snapshot.as_deref(),
    )?;

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

    if args.verbose {
        for mismatch in &report.mismatches {
            println!("mismatch    {mismatch}");
        }
    }

    if report.checked == 0 {
        println!("note        nothing was checked, so nothing is verified");
    }

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
