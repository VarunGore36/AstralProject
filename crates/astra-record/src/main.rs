use std::path::{Path, PathBuf};
use std::process::ExitCode;

use astra_types::{
    CaptureId, CaptureManifest, Channel, Instrument, MarketType, SCHEMA_VERSION, Symbol, Timestamp,
    Venue,
};
use clap::{Parser, Subcommand};
use thiserror::Error;

const MANIFEST_FILE: &str = "manifest.json";
const FRAMES_DIR: &str = "frames";

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

#[derive(Debug, Error)]
enum RecordError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialisation error: {0}")]
    Serialisation(#[from] serde_json::Error),
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
    }
}

fn init(args: InitArgs) -> Result<(), RecordError> {
    let manifest = write_capture_layout(
        &args.output,
        args.venue,
        args.market,
        args.symbol,
        args.channel,
    )?;

    println!("capture_id  {}", manifest.capture_id.as_str());
    println!("output      {}", args.output.display());
    println!(
        "instrument  {} {} {}",
        manifest.instrument.venue(),
        manifest.instrument.market_type(),
        manifest.instrument.symbol()
    );
    println!("channel     {}", manifest.channel);
    println!("frames      {}", manifest.frames_written);
    println!("manifest    {}", args.output.join(MANIFEST_FILE).display());

    Ok(())
}

fn write_capture_layout(
    output: &Path,
    venue: Venue,
    market_type: MarketType,
    symbol: Symbol,
    channel: Channel,
) -> Result<CaptureManifest, RecordError> {
    std::fs::create_dir_all(output.join(FRAMES_DIR))?;

    let manifest = CaptureManifest {
        schema_version: SCHEMA_VERSION,
        capture_id: CaptureId::new(uuid::Uuid::new_v4().to_string()),
        created_at: Timestamp::now(),
        instrument: Instrument::new(venue, market_type, symbol),
        channel,
        frames_written: 0,
    };

    std::fs::write(
        output.join(MANIFEST_FILE),
        serde_json::to_string_pretty(&manifest)?,
    )?;

    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_output(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("astra-record-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn init_writes_layout_and_manifest() {
        let output = temp_output("init");
        let manifest = write_capture_layout(
            &output,
            Venue::Binance,
            MarketType::PerpUsdt,
            Symbol::new("BTC/USDT").unwrap(),
            Channel::BookDiff,
        )
        .unwrap();

        assert!(output.join(FRAMES_DIR).is_dir());

        let body = std::fs::read_to_string(output.join(MANIFEST_FILE)).unwrap();
        let decoded: CaptureManifest = serde_json::from_str(&body).unwrap();
        assert_eq!(decoded, manifest);
        assert_eq!(decoded.schema_version, SCHEMA_VERSION);
        assert_eq!(decoded.frames_written, 0);
        assert_eq!(decoded.instrument.symbol().as_str(), "BTC/USDT");

        std::fs::remove_dir_all(&output).unwrap();
    }
}
