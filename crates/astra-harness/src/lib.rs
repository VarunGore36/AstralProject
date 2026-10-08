//! Benchmark harness (`bench-v1`): one capture plus one config in, one
//! hashed report out.
//!
//! The capture is replayed exactly once with a composed strategy: `book-top`
//! maintains the book and emits top-of-book signals while a trade collector
//! gathers prints and gaps. Every probe then simulates over the collected
//! stream. The report is canonical JSON — field order is file order, no maps,
//! no timestamps — hashed with SHA-256 over the exact bytes written.
//!
//! See `docs/benchmark-harness.md` for the full contract.

use std::path::Path;

use astra_exec::{LimitOrder, OrderOutcome, Side, TradeCollector, simulate};
use astra_replay::{BookTop, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Harness version stamped on every report. Bump on any rule change.
pub const HARNESS_VERSION: &str = "bench-v1";

/// The only replay strategy v1 knows. Unknown names are refused, never
/// defaulted: a benchmark that runs a different strategy than intended is
/// worse than one that refuses to run.
const SUPPORTED_STRATEGY: &str = "book-top";

#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("config error: {0}")]
    Config(String),
    #[error("replay error: {0}")]
    Replay(#[from] astra_replay::ReplayError),
    #[error("exec error: {0}")]
    Exec(#[from] astra_exec::ExecError),
    #[error("report error: {0}")]
    Report(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeSpec {
    side: StrategySide,
    price: astra_types::Fixed,
    quantity: astra_types::Fixed,
    fee_bps: u32,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum StrategySide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BenchmarkConfig {
    version: u32,
    seed: u64,
    strategy: String,
    #[serde(default)]
    probes: Vec<ProbeSpec>,
}

#[derive(Clone, Debug, Serialize)]
struct ProbeResult {
    side: String,
    price: astra_types::Fixed,
    quantity: astra_types::Fixed,
    fee_bps: u32,
    #[serde(flatten)]
    outcome: OrderOutcome,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkReport {
    harness_version: &'static str,
    exec_version: &'static str,
    capture_id: String,
    frames: u64,
    seed: u64,
    strategy: String,
    signal_hash: String,
    probes: Vec<ProbeResult>,
}

/// One replay pass driving both the book strategy and the trade collector.
struct HarnessStrategy {
    book: BookTop,
    trades: TradeCollector,
}

impl astra_replay::Strategy for HarnessStrategy {
    fn on_book_diff(&mut self, event: &astra_replay::BookDiffEvent, ctx: &mut Context) {
        self.book.on_book_diff(event, ctx);
    }
    fn on_snapshot(&mut self, event: &astra_replay::SnapshotEvent, ctx: &mut Context) {
        self.book.on_snapshot(event, ctx);
    }
    fn on_trade(&mut self, event: &astra_replay::TradeEvent, ctx: &mut Context) {
        self.trades.on_trade(event, ctx);
    }
    fn on_top_of_book(&mut self, event: &astra_replay::TopBookEvent, ctx: &mut Context) {
        self.trades.on_top_of_book(event, ctx);
    }
    fn on_gap(&mut self, marker: &astra_types::GapMarker, ctx: &mut Context) {
        self.book.on_gap(marker, ctx);
        self.trades.on_gap(marker, ctx);
    }
    fn on_end(&mut self, frames: u64, ctx: &mut Context) {
        self.book.on_end(frames, ctx);
        self.trades.on_end(frames, ctx);
    }
}

/// What one benchmark run produces: the exact report bytes, their hash,
/// and the counts the CLI prints.
pub struct BenchmarkRun {
    pub bytes: Vec<u8>,
    pub hash: String,
    pub probes: u64,
    pub fills: u64,
    pub trades: u64,
    pub gaps: u64,
}
fn side_name(side: StrategySide) -> &'static str {
    match side {
        StrategySide::Buy => "buy",
        StrategySide::Sell => "sell",
    }
}

/// Run the benchmark: one replay pass, N probe simulations, one canonical
/// report. Returns the exact report bytes (what hits disk), their hash, and
/// the counts the CLI prints.
pub fn run_benchmark(input: &Path, config_json: &str) -> Result<BenchmarkRun, HarnessError> {
    let config: BenchmarkConfig = serde_json::from_str(config_json)
        .map_err(|error| HarnessError::Config(error.to_string()))?;
    if config.version != 1 {
        return Err(HarnessError::Config(format!(
            "unsupported config version {}, this build reads 1",
            config.version
        )));
    }
    if config.strategy != SUPPORTED_STRATEGY {
        return Err(HarnessError::Config(format!(
            "unsupported strategy {:?}, this build runs {:?}",
            config.strategy, SUPPORTED_STRATEGY
        )));
    }

    let mut strategy = HarnessStrategy {
        book: BookTop::new(),
        trades: TradeCollector::default(),
    };
    let replay_report = astra_replay::replay(input, config.seed, &mut strategy)?;

    let mut probes = Vec::with_capacity(config.probes.len());
    let mut fills = 0u64;
    for spec in &config.probes {
        let order = LimitOrder {
            side: match spec.side {
                StrategySide::Buy => Side::Buy,
                StrategySide::Sell => Side::Sell,
            },
            price: spec.price,
            quantity: spec.quantity,
        };
        let outcome = simulate(&order, spec.fee_bps, &strategy.trades.events)?;
        if matches!(outcome, OrderOutcome::Filled { .. }) {
            fills += 1;
        }
        probes.push(ProbeResult {
            side: side_name(spec.side).to_owned(),
            price: spec.price,
            quantity: spec.quantity,
            fee_bps: spec.fee_bps,
            outcome,
        });
    }

    let manifest = read_capture_id(input)?;
    let report = BenchmarkReport {
        harness_version: HARNESS_VERSION,
        exec_version: astra_exec::MODEL_VERSION,
        capture_id: manifest.0,
        frames: manifest.1,
        seed: config.seed,
        strategy: config.strategy.clone(),
        signal_hash: replay_report.signal_hash.clone(),
        probes,
    };
    let mut bytes = serde_json::to_string_pretty(&report)?;
    bytes.push('\n');
    let hash = hash_hex(&bytes);

    Ok(BenchmarkRun {
        bytes: bytes.into_bytes(),
        hash,
        probes: config.probes.len() as u64,
        fills,
        trades: strategy.trades.trades,
        gaps: strategy.trades.gaps,
    })
}

fn hash_hex(bytes: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_capture_id(input: &Path) -> Result<(String, u64), HarnessError> {
    // The replay above already enforced the manifest count and schema
    // version; this read only extracts the report fields. The version is
    // still checked here so the failure names the cause if the two ever drift.
    let body = std::fs::read_to_string(input.join(astra_record::capture::MANIFEST_FILE))?;
    let manifest: serde_json::Value =
        serde_json::from_str(&body).map_err(|error| HarnessError::Config(error.to_string()))?;
    let version = manifest
        .get("schema_version")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| HarnessError::Config("manifest has no schema_version".to_owned()))?;
    if version != u64::from(astra_types::SCHEMA_VERSION) {
        return Err(HarnessError::Config(format!(
            "unsupported capture schema version {version}"
        )));
    }
    let capture_id = manifest
        .get("capture_id")
        .and_then(|value| value.as_str())
        .ok_or_else(|| HarnessError::Config("manifest has no capture_id".to_owned()))?
        .to_owned();
    let frames = manifest
        .get("frames_written")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| HarnessError::Config("manifest has no frames_written".to_owned()))?;
    Ok((capture_id, frames))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_config_fields_are_refused() {
        let config = r#"{"version":1,"seed":7,"strategy":"book-top","probes":[],"extra":1}"#;
        assert!(matches!(
            run_benchmark(std::path::Path::new("./missing"), config),
            Err(HarnessError::Config(_))
        ));
    }

    #[test]
    fn unknown_strategies_and_versions_are_refused() {
        let config = r#"{"version":1,"seed":7,"strategy":"rsi-9000","probes":[]}"#;
        assert!(matches!(
            run_benchmark(std::path::Path::new("./missing"), config),
            Err(HarnessError::Config(_))
        ));

        let config = r#"{"version":2,"seed":7,"strategy":"book-top","probes":[]}"#;
        assert!(matches!(
            run_benchmark(std::path::Path::new("./missing"), config),
            Err(HarnessError::Config(_))
        ));
    }
}
