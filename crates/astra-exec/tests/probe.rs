//! End-to-end: real-format trade frames on disk, replayed through a probe.

use std::path::{Path, PathBuf};

use astra_exec::{LimitOrder, OrderOutcome, Side, UnfilledReason, probe_capture};
use astra_record::capture::{FRAMES_DIR, write_manifest};
use astra_record::store::ChunkWriter;
use astra_types::{
    CaptureFlags, CaptureId, CaptureManifest, CaptureRecord, Channel, GapMarker, Instrument,
    MarketType, SCHEMA_VERSION, Symbol, Timestamp, Venue,
};

fn binance_instrument() -> Instrument {
    Instrument::new(
        Venue::Binance,
        MarketType::Spot,
        Symbol::new("BTC/USDT").unwrap(),
    )
}

fn binance_trade(seq: u64, trade_id: u64, price: &str, buyer_is_maker: bool) -> CaptureRecord {
    let payload = format!(
        r#"{{"e":"trade","E":1700000000000,"s":"BTCUSDT","t":{trade_id},"p":"{price}","q":"0.50000000","T":1700000000000,"m":{buyer_is_maker}}}"#
    );
    CaptureRecord {
        seq,
        instrument: binance_instrument(),
        channel: Channel::Trade,
        ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000 + seq as i64),
        ts_exchange: None,
        payload: payload.into_bytes(),
        flags: CaptureFlags::NONE,
    }
}

fn gap_record(seq: u64) -> CaptureRecord {
    let marker = GapMarker {
        started_at: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
        ended_at: Timestamp::from_unix_nanos(1_700_000_000_100_000_000),
        attempts: 1,
        reason: "venue_close".to_owned(),
    };
    CaptureRecord {
        seq,
        instrument: binance_instrument(),
        channel: Channel::Trade,
        ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000 + seq as i64),
        ts_exchange: None,
        payload: serde_json::to_vec(&marker).unwrap(),
        flags: CaptureFlags::SYNTHETIC
            .union(CaptureFlags::SEQUENCE_GAP)
            .union(CaptureFlags::UNRELIABLE),
    }
}

fn write_capture(dir: &Path, records: Vec<CaptureRecord>) {
    let venue_frames = records
        .iter()
        .filter(|record| !record.flags.contains(CaptureFlags::SYNTHETIC))
        .count() as u64;
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
            capture_id: CaptureId::new("probe-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: binance_instrument(),
            channel: Channel::Trade,
            frames_written: venue_frames,
            stop_reason: None,
        },
    )
    .unwrap();
}

fn temp_directory(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("astra-exec-probe-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}

fn buy(limit: &str) -> LimitOrder {
    LimitOrder {
        side: Side::Buy,
        price: limit.parse().unwrap(),
        quantity: "1".parse().unwrap(),
    }
}

#[test]
fn a_probe_fill_cites_its_through_print() {
    let dir = temp_directory("fill");
    write_capture(
        &dir,
        vec![
            binance_trade(0, 1, "101.00000000", false),
            binance_trade(1, 2, "99.50000000", true),
        ],
    );

    let report = probe_capture(&dir, 7, &buy("100.00000000"), 5).unwrap();

    assert_eq!(report.trades, 2);
    assert_eq!(report.gaps, 0);
    let OrderOutcome::Filled {
        fill_price,
        fee,
        print_seq,
        ..
    } = report.outcome
    else {
        panic!("expected a fill, got {:?}", report.outcome);
    };
    assert_eq!(fill_price.to_string(), "100.00000000");
    assert_eq!(fee.to_string(), "0.05000000");
    assert_eq!(print_seq, 1);

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_probe_honours_gaps_and_expiry() {
    let gapped = temp_directory("gapped");
    write_capture(
        &gapped,
        vec![
            binance_trade(0, 1, "101.00000000", false),
            gap_record(1),
            binance_trade(2, 2, "99.50000000", true),
        ],
    );
    let report = probe_capture(&gapped, 7, &buy("100.00000000"), 5).unwrap();
    assert_eq!(report.trades, 2);
    assert_eq!(report.gaps, 1);
    assert_eq!(
        report.outcome,
        OrderOutcome::Unfilled {
            reason: UnfilledReason::VoidedByGap
        }
    );
    std::fs::remove_dir_all(&gapped).unwrap();

    let quiet = temp_directory("quiet");
    write_capture(&quiet, vec![binance_trade(0, 1, "101.00000000", false)]);
    let report = probe_capture(&quiet, 7, &buy("100.00000000"), 5).unwrap();
    assert_eq!(
        report.outcome,
        OrderOutcome::Unfilled {
            reason: UnfilledReason::NoThroughPrint
        }
    );
    std::fs::remove_dir_all(&quiet).unwrap();
}
