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
    let records = vec![
        book_frame(0),
        trade_frame(1, 1, "101.00000000"),
        trade_frame(2, 2, "99.00000000"),
    ];
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
