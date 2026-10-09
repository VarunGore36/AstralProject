//! End-to-end: one capture plus one config in, one hashed report out — twice.

use std::path::{Path, PathBuf};

use astra_harness::run_benchmark;
use astra_record::capture::{FRAMES_DIR, write_manifest};
use astra_record::store::ChunkWriter;
use astra_types::{
    CaptureFlags, CaptureId, CaptureManifest, CaptureRecord, Channel, Instrument, MarketType,
    SCHEMA_VERSION, Symbol, Timestamp, Venue,
};

const CONFIG: &str = r#"{"version":1,"seed":7,"strategy":"book-top","probes":[
{"side":"buy","price":"100.00000000","quantity":"1","fee_bps":5},
{"side":"sell","price":"150.00000000","quantity":"2","fee_bps":10}
]}"#;

fn instrument() -> Instrument {
    Instrument::new(
        Venue::Binance,
        MarketType::Spot,
        Symbol::new("BTC/USDT").unwrap(),
    )
}

fn book_frame(seq: u64) -> CaptureRecord {
    CaptureRecord {
        seq,
        instrument: instrument(),
        channel: Channel::BookDiff,
        ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000 + seq as i64),
        ts_exchange: None,
        payload: br#"{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":100,"u":105,"b":[["99.00000000","1"]],"a":[["101.00000000","1"]]}"#.to_vec(),
        flags: CaptureFlags::NONE,
    }
}

fn trade_frame(seq: u64, trade_id: u64, price: &str) -> CaptureRecord {
    let payload = format!(
        r#"{{"e":"trade","E":1700000000000,"s":"BTCUSDT","t":{trade_id},"p":"{price}","q":"1.00000000","T":1700000000000,"m":false}}"#
    );
    CaptureRecord {
        seq,
        instrument: instrument(),
        channel: Channel::Trade,
        ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000 + seq as i64),
        ts_exchange: None,
        payload: payload.into_bytes(),
        flags: CaptureFlags::NONE,
    }
}

fn write_capture(dir: &Path) {
    write_capture_priced(dir, &["101.00000000", "99.00000000"]);
}

fn write_capture_priced(dir: &Path, prices: &[&str]) {
    let mut records = vec![book_frame(0)];
    for (index, price) in prices.iter().enumerate() {
        records.push(trade_frame(index as u64 + 1, index as u64 + 1, price));
    }
    std::fs::create_dir_all(dir.join(FRAMES_DIR)).unwrap();
    let mut writer = ChunkWriter::open(dir.join(FRAMES_DIR), 100).unwrap();
    for record in &records {
        writer.append(record).unwrap();
    }
    writer.finish().unwrap();
    write_manifest(
        dir,
        &CaptureManifest {
            schema_version: SCHEMA_VERSION,
            capture_id: CaptureId::new("bench-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument(),
            channel: Channel::BookDiff,
            frames_written: records.len() as u64,
            stop_reason: None,
        },
    )
    .unwrap();
}

fn temp_directory(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("astra-harness-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}

#[test]
fn same_inputs_yield_byte_identical_reports() {
    let dir = temp_directory("deterministic");
    write_capture(&dir);

    let first = run_benchmark(&dir, CONFIG).unwrap();
    let second = run_benchmark(&dir, CONFIG).unwrap();

    assert_eq!(first.bytes, second.bytes);
    assert_eq!(first.hash, second.hash);
    assert_eq!(first.probes, 2);
    assert_eq!(first.fills, 1);
    assert_eq!(first.trades, 2);
    assert_eq!(first.gaps, 0);

    let text = String::from_utf8(first.bytes).unwrap();
    for key in [
        "\"harness_version\": \"bench-v1\"",
        "\"exec_version\": \"exec-v1\"",
        "\"capture_id\": \"bench-test\"",
        "\"signal_hash\":",
        "\"print_seq\": 2",
    ] {
        assert!(text.contains(key), "missing {key}\n{text}");
    }
    assert!(text.ends_with('\n'));

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_torn_manifest_refuses_the_run() {
    let dir = temp_directory("torn");
    write_capture(&dir);

    let manifest_path = dir.join("manifest.json");
    let body = std::fs::read_to_string(&manifest_path).unwrap();
    let mut manifest: serde_json::Value = serde_json::from_str(&body).unwrap();
    manifest["frames_written"] = serde_json::json!(99);
    std::fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

    assert!(run_benchmark(&dir, CONFIG).is_err());
    assert!(!dir.join("report.json").exists());

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn record_list_verify_roundtrip() {
    use astra_harness::{list_runs, record_run, verify_run};

    let dir = temp_directory("registry");
    let registry = temp_directory("registry-store");
    write_capture(&dir);

    let run = run_benchmark(&dir, CONFIG).unwrap();
    let stored = record_run(&registry, CONFIG, &run).unwrap();
    assert!(stored.join("report.json").is_file());
    assert!(stored.join("config.json").is_file());

    // Re-recording is idempotent: same hash, still one entry.
    record_run(&registry, CONFIG, &run).unwrap();
    let runs = list_runs(&registry).unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].hash, run.hash);
    assert_eq!(runs[0].probes, 2);
    assert_eq!(runs[0].fills, 1);

    // Same bytes reproduce; different outcomes do not. (Note: merely
    // different prints with identical outcomes verify true — correctly, a
    // report covers outcomes, not bytes.)
    assert!(verify_run(&registry, &run.hash, &dir).unwrap());
    let other = temp_directory("registry-other");
    write_capture_priced(&other, &["101.00000000", "102.00000000"]);
    assert!(!verify_run(&registry, &run.hash, &other).unwrap());

    // Unknown hashes fail loudly, not silently.
    assert!(verify_run(&registry, &"0".repeat(64), &dir).is_err());
    // An empty directory is an empty registry, not an error.
    assert!(
        list_runs(&temp_directory("registry-empty"))
            .unwrap()
            .is_empty()
    );

    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_dir_all(&other).unwrap();
    std::fs::remove_dir_all(&registry).unwrap();
}

#[test]
fn distinct_runs_list_sorted_side_by_side() {
    use astra_harness::{list_runs, record_run, run_benchmark};

    let dir = temp_directory("multi");
    let registry = temp_directory("multi-store");
    write_capture(&dir);

    let first = run_benchmark(&dir, CONFIG).unwrap();
    let other_config = CONFIG.replace("100.00000000", "90.00000000");
    let second = run_benchmark(&dir, &other_config).unwrap();
    assert_ne!(first.hash, second.hash);

    record_run(&registry, CONFIG, &first).unwrap();
    record_run(&registry, &other_config, &second).unwrap();

    let runs = list_runs(&registry).unwrap();
    assert_eq!(runs.len(), 2);
    let mut hashes: Vec<&str> = runs.iter().map(|run| run.hash.as_str()).collect();
    hashes.sort_unstable();
    assert_eq!(
        hashes,
        runs.iter().map(|run| run.hash.as_str()).collect::<Vec<_>>()
    );

    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_dir_all(&registry).unwrap();
}

#[test]
fn trade_tally_strategy_benchmarks_deterministically() {
    let dir = temp_directory("tally");
    write_capture(&dir);
    let config = r#"{"version":1,"seed":7,"strategy":"trade-tally","probes":[
{"side":"buy","price":"100.00000000","quantity":"1","fee_bps":5}
]}"#;

    let first = run_benchmark(&dir, config).unwrap();
    let second = run_benchmark(&dir, config).unwrap();

    assert_eq!(first.bytes, second.bytes);
    assert_eq!(first.fills, 1);
    let text = String::from_utf8(first.bytes).unwrap();
    assert!(text.contains("\"strategy\": \"trade-tally\""), "{text}");

    // A different strategy observes the same prints but emits different
    // signals: same probes, different signal hash.
    let book_config = config.replace("trade-tally", "book-top");
    let book = run_benchmark(&dir, &book_config).unwrap();
    let book_text = String::from_utf8(book.bytes).unwrap();
    let signal_of = |text: &str| {
        text.lines()
            .find(|line| line.contains("signal_hash"))
            .unwrap()
            .to_owned()
    };
    assert_ne!(signal_of(&text), signal_of(&book_text));

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_tampered_registry_entry_fails_verification() {
    use astra_harness::{record_run, run_benchmark, verify_run};

    let dir = temp_directory("tamper-entry");
    let registry = temp_directory("tamper-store");
    write_capture(&dir);

    let run = run_benchmark(&dir, CONFIG).unwrap();
    record_run(&registry, CONFIG, &run).unwrap();
    assert!(verify_run(&registry, &run.hash, &dir).unwrap());

    // Rewrite the stored config with a different probe price: the re-run
    // disagrees with the recorded hash, so verification fails closed.
    let stored = registry.join(&run.hash).join("config.json");
    let doctored = CONFIG.replace("100.00000000", "10.00000000");
    std::fs::write(&stored, doctored).unwrap();

    assert!(!verify_run(&registry, &run.hash, &dir).unwrap());

    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_dir_all(&registry).unwrap();
}
