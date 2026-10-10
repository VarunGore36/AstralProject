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

use std::path::{Path, PathBuf};

use astra_exec::{LimitOrder, OrderOutcome, Side, TradeCollector, simulate};
use astra_replay::{BookTop, Context, TradeTally};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Harness version stamped on every report. Bump on any rule change.
pub const HARNESS_VERSION: &str = "bench-v1";

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
enum ReplayStrategy {
    BookTop(BookTop),
    TradeTally(TradeTally),
}

struct HarnessStrategy {
    strategy: ReplayStrategy,
    trades: TradeCollector,
}

impl HarnessStrategy {
    fn for_name(name: &str) -> Result<Self, HarnessError> {
        let strategy = match name {
            "book-top" => ReplayStrategy::BookTop(BookTop::new()),
            "trade-tally" => ReplayStrategy::TradeTally(TradeTally::new()),
            other => {
                return Err(HarnessError::Config(format!(
                    "unsupported strategy {other:?}, this build runs \"book-top\" and \"trade-tally\""
                )));
            }
        };
        Ok(HarnessStrategy {
            strategy,
            trades: TradeCollector::default(),
        })
    }
}

impl astra_replay::Strategy for HarnessStrategy {
    fn on_book_diff(&mut self, event: &astra_replay::BookDiffEvent, ctx: &mut Context) {
        match &mut self.strategy {
            ReplayStrategy::BookTop(book) => book.on_book_diff(event, ctx),
            ReplayStrategy::TradeTally(tally) => tally.on_book_diff(event, ctx),
        }
    }
    fn on_snapshot(&mut self, event: &astra_replay::SnapshotEvent, ctx: &mut Context) {
        match &mut self.strategy {
            ReplayStrategy::BookTop(book) => book.on_snapshot(event, ctx),
            ReplayStrategy::TradeTally(tally) => tally.on_snapshot(event, ctx),
        }
    }
    fn on_trade(&mut self, event: &astra_replay::TradeEvent, ctx: &mut Context) {
        self.trades.on_trade(event, ctx);
    }
    fn on_top_of_book(&mut self, event: &astra_replay::TopBookEvent, ctx: &mut Context) {
        self.trades.on_top_of_book(event, ctx);
    }
    fn on_gap(&mut self, marker: &astra_types::GapMarker, ctx: &mut Context) {
        match &mut self.strategy {
            ReplayStrategy::BookTop(book) => book.on_gap(marker, ctx),
            ReplayStrategy::TradeTally(tally) => tally.on_gap(marker, ctx),
        }
        self.trades.on_gap(marker, ctx);
    }
    fn on_end(&mut self, frames: u64, ctx: &mut Context) {
        match &mut self.strategy {
            ReplayStrategy::BookTop(book) => book.on_end(frames, ctx),
            ReplayStrategy::TradeTally(tally) => tally.on_end(frames, ctx),
        }
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
    let mut strategy = HarnessStrategy::for_name(&config.strategy)?;
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

/// One registry entry: what is listed and what verification compares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub hash: String,
    pub capture_id: String,
    pub seed: u64,
    pub probes: u64,
    pub fills: u64,
}

/// Record a run: store its report plus the config that produced it under
/// `<registry>/<report_hash>/`. Content-addressed, so re-recording the same
/// run is a no-op and history can never fork.
pub fn record_run(
    registry: &Path,
    config_json: &str,
    run: &BenchmarkRun,
) -> Result<PathBuf, HarnessError> {
    let dir = registry.join(&run.hash);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("report.json"), &run.bytes)?;
    std::fs::write(dir.join("config.json"), config_json)?;
    Ok(dir)
}

/// List every recorded run, sorted by hash for stable output.
pub fn list_runs(registry: &Path) -> Result<Vec<RunSummary>, HarnessError> {
    let mut runs = Vec::new();
    let entries = match std::fs::read_dir(registry) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(runs),
        Err(error) => return Err(HarnessError::Io(error)),
    };
    for entry in entries {
        let entry = entry?;
        let report_path = entry.path().join("report.json");
        if !report_path.is_file() {
            continue;
        }
        // Directory names that are not UTF-8 can never be report hashes the
        // registry recorded (hex is ASCII); skip them instead of listing a
        // phantom entry no verification could ever match.
        let Some(hash) = entry.file_name().to_str().map(|name| name.to_owned()) else {
            continue;
        };
        let body = std::fs::read_to_string(&report_path)?;
        let report: serde_json::Value =
            serde_json::from_str(&body).map_err(HarnessError::Report)?;
        let get_str = |key: &str| {
            report
                .get(key)
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_owned()
        };
        let get_u64 = |key: &str| {
            report
                .get(key)
                .and_then(|value| value.as_u64())
                .unwrap_or(0)
        };
        let probe_list = report.get("probes").and_then(|value| value.as_array());
        runs.push(RunSummary {
            hash,
            capture_id: get_str("capture_id"),
            seed: get_u64("seed"),
            probes: probe_list.map(|probes| probes.len() as u64).unwrap_or(0),
            fills: probe_list
                .map(|probes| {
                    probes
                        .iter()
                        .filter(|probe| {
                            probe.get("result").and_then(|r| r.as_str()) == Some("filled")
                        })
                        .count() as u64
                })
                .unwrap_or(0),
        });
    }
    runs.sort_by(|a, b| a.hash.cmp(&b.hash));
    Ok(runs)
}

/// Render a benchmark run report exactly as the CLI prints it.
///
/// Golden-tested with the other report printers.
pub fn format_run_report(input: &Path, config: &Path, output: &Path, run: &BenchmarkRun) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "input       {}", input.display());
    let _ = writeln!(out, "config      {}", config.display());
    let _ = writeln!(out, "probes      {}", run.probes);
    let _ = writeln!(out, "fills       {}", run.fills);
    let _ = writeln!(out, "report      {}", output.display());
    let _ = writeln!(out, "report_hash {}", run.hash);

    out
}

/// Render a registry listing exactly as the CLI prints it.
pub fn format_list_report(registry: &Path, runs: &[RunSummary]) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "registry    {}", registry.display());
    let _ = writeln!(out, "runs        {}", runs.len());
    for run in runs {
        let _ = writeln!(
            out,
            "run         {} probes={} fills={} seed={} capture={}",
            run.hash, run.probes, run.fills, run.seed, run.capture_id
        );
    }

    out
}

/// Render a reproduction verdict exactly as the CLI prints it.
pub fn format_verify_report(registry: &Path, hash: &str, input: &Path, reproduced: bool) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "registry    {}", registry.display());
    let _ = writeln!(out, "hash        {hash}");
    let _ = writeln!(out, "input       {}", input.display());
    let _ = writeln!(
        out,
        "verdict     {}",
        if reproduced {
            "reproduced"
        } else {
            "MISMATCH, see above"
        }
    );

    out
}

/// Reproduce a recorded run: re-execute its stored config against `input`
/// and compare hashes. Returns true on exact reproduction.
pub fn verify_run(registry: &Path, hash: &str, input: &Path) -> Result<bool, HarnessError> {
    // The hash becomes a path segment below. Refuse anything that is not a
    // report hash (64 lowercase hex chars) so values like `../../etc` can
    // never escape the registry directory.
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(HarnessError::Config(format!("not a report hash: {hash:?}")));
    }
    let dir = registry.join(hash);
    let config = std::fs::read_to_string(dir.join("config.json"))?;
    let run = run_benchmark(input, &config)?;
    Ok(run.hash == hash)
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

    #[test]
    fn non_hash_inputs_never_become_paths() {
        let registry = std::path::Path::new("./missing-registry");
        let input = std::path::Path::new("./missing-capture");
        // Traversal, empties, wrong lengths, and uppercase hex are refused
        // as malformed hashes before any filesystem access.
        let bad_inputs = vec![
            "../..".to_owned(),
            String::new(),
            "abc".to_owned(),
            "f".repeat(63),
            "F".repeat(64),
        ];
        for bad in &bad_inputs {
            assert!(
                matches!(
                    verify_run(registry, bad, input),
                    Err(HarnessError::Config(_))
                ),
                "input {bad:?} should be refused"
            );
        }
        // A well-formed but absent hash fails on the missing directory, not
        // on validation — proving the check above is what guards the join.
        assert!(matches!(
            verify_run(registry, &"0".repeat(64), input),
            Err(HarnessError::Io(_))
        ));
    }

    #[test]
    fn non_utf8_directories_list_as_nothing() {
        use std::os::unix::ffi::OsStringExt;

        let registry =
            std::env::temp_dir().join(format!("astra-harness-nonutf8-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&registry);
        let odd = registry.join(std::ffi::OsString::from_vec(vec![0xFF, 0xFE]));
        std::fs::create_dir_all(&odd).unwrap();
        std::fs::write(odd.join("report.json"), "{}").unwrap();

        // A directory whose name is not UTF-8 can never be a recorded hash;
        // it is skipped instead of listed as a phantom empty-hash entry.
        assert!(list_runs(&registry).unwrap().is_empty());

        std::fs::remove_dir_all(&registry).unwrap();
    }

    #[test]
    fn the_report_formats_are_pinned_line_by_line() {
        let run = BenchmarkRun {
            bytes: b"{}\n".to_vec(),
            hash: "9f2c".to_owned(),
            probes: 2,
            fills: 1,
            trades: 10,
            gaps: 0,
        };
        let text = format_run_report(
            std::path::Path::new("./capture"),
            std::path::Path::new("./bench.json"),
            std::path::Path::new("./report.json"),
            &run,
        );
        for line in [
            "input       ./capture",
            "config      ./bench.json",
            "probes      2",
            "fills       1",
            "report      ./report.json",
            "report_hash 9f2c",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }

        let runs = vec![RunSummary {
            hash: "9f2c".to_owned(),
            capture_id: "bench-test".to_owned(),
            seed: 7,
            probes: 2,
            fills: 1,
        }];
        let text = format_list_report(std::path::Path::new("./experiments"), &runs);
        for line in [
            "registry    ./experiments",
            "runs        1",
            "run         9f2c probes=2 fills=1 seed=7 capture=bench-test",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }

        let text = format_verify_report(
            std::path::Path::new("./experiments"),
            "9f2c",
            std::path::Path::new("./capture"),
            true,
        );
        assert!(text.contains("verdict     reproduced"), "{text}");
        let text = format_verify_report(
            std::path::Path::new("./experiments"),
            "9f2c",
            std::path::Path::new("./capture"),
            false,
        );
        assert!(text.contains("verdict     MISMATCH, see above"), "{text}");
    }
}
