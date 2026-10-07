use std::path::Path;

use rand::SeedableRng;
use rand::rngs::StdRng;
use sha2::{Digest, Sha256};
use thiserror::Error;

use astra_book::{BookDiff, BookSnapshot, UpdateSpan};
use astra_record::capture::{FRAMES_DIR, MANIFEST_FILE};
use astra_record::feed;
use astra_record::store;
use astra_types::{CaptureFlags, CaptureManifest, Channel, GapMarker, Timestamp};

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("store error: {0}")]
    Store(#[from] store::StoreError),
    #[error("manifest error: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("malformed record: {0}")]
    Malformed(String),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Signal {
    pub tag: String,
    pub payload: Vec<u8>,
}

pub struct Context {
    now: Timestamp,
    rng: StdRng,
    signals: Vec<Signal>,
}

impl Context {
    pub fn now(&self) -> Timestamp {
        self.now
    }

    pub fn rng(&mut self) -> &mut StdRng {
        &mut self.rng
    }

    pub fn emit(&mut self, tag: &str, payload: Vec<u8>) {
        self.signals.push(Signal {
            tag: tag.to_owned(),
            payload,
        });
    }

    pub fn signals(&self) -> &[Signal] {
        &self.signals
    }

    #[cfg(test)]
    pub fn for_tests(seed: u64) -> Self {
        Context {
            now: Timestamp::from_unix_nanos(0),
            rng: StdRng::seed_from_u64(seed),
            signals: Vec::new(),
        }
    }
}

pub trait Strategy {
    fn on_book_diff(&mut self, event: &BookDiffEvent, ctx: &mut Context);
    fn on_snapshot(&mut self, event: &SnapshotEvent, ctx: &mut Context);
    fn on_trade(&mut self, _event: &TradeEvent, _ctx: &mut Context) {}
    fn on_top_of_book(&mut self, _event: &TopBookEvent, _ctx: &mut Context) {}
    fn on_gap(&mut self, marker: &GapMarker, ctx: &mut Context);
    fn on_end(&mut self, _frames: u64, _ctx: &mut Context) {}
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BookDiffEvent {
    pub seq: u64,
    pub ts_socket: Timestamp,
    pub ts_exchange: Option<Timestamp>,
    pub span: UpdateSpan,
    pub diff: BookDiff,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SnapshotEvent {
    pub seq: u64,
    pub ts_socket: Timestamp,
    pub snapshot: BookSnapshot,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TradeEvent {
    pub seq: u64,
    pub print_index: u32,
    pub ts_socket: Timestamp,
    pub ts_exchange: Option<Timestamp>,
    pub trade: feed::TradePrint,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TopBookEvent {
    pub seq: u64,
    pub ts_socket: Timestamp,
    pub ts_exchange: Option<Timestamp>,
    pub top: feed::TopOfBook,
}

#[derive(Clone, Debug, Default)]
pub struct ReplayReport {
    pub frames: u64,
    pub events_emitted: u64,
    pub trade_events: u64,
    pub top_book_events: u64,
    pub gaps: u64,
    pub skipped_channel: u64,
    pub skipped_unparseable: u64,
    pub signals: u64,
    pub signal_hash: String,
}

pub fn replay(
    input: &Path,
    seed: u64,
    strategy: &mut dyn Strategy,
) -> Result<ReplayReport, ReplayError> {
    let manifest = read_manifest(input)?;
    let records = store::read_all(&input.join(FRAMES_DIR))?;

    let venue_frames: u64 = records
        .iter()
        .filter(|record| !record.flags.contains(CaptureFlags::SYNTHETIC))
        .count() as u64;
    if venue_frames != manifest.frames_written {
        return Err(ReplayError::Malformed(format!(
            "manifest claims {} venue frames but the capture holds {}",
            manifest.frames_written, venue_frames
        )));
    }

    let mut ctx = Context {
        now: Timestamp::from_unix_nanos(0),
        rng: StdRng::seed_from_u64(seed),
        signals: Vec::new(),
    };
    let mut report = ReplayReport::default();

    for record in &records {
        report.frames += 1;
        ctx.now = record.ts_socket;

        if record.flags.contains(CaptureFlags::SYNTHETIC) {
            let marker: GapMarker = serde_json::from_slice(&record.payload).map_err(|error| {
                ReplayError::Malformed(format!("seq {}: bad gap marker: {error}", record.seq))
            })?;
            strategy.on_gap(&marker, &mut ctx);
            report.gaps += 1;
            report.events_emitted += 1;
            continue;
        }

        if record.channel != Channel::BookDiff
            && record.channel != Channel::Trade
            && record.channel != Channel::BookTicker
        {
            report.skipped_channel += 1;
            continue;
        }

        let venue = record.instrument.venue();

        if record.channel == Channel::Trade {
            match feed::trade_prints(venue, record.channel, &record.payload) {
                Some(prints) if !prints.is_empty() => {
                    for (index, print) in prints.into_iter().enumerate() {
                        strategy.on_trade(
                            &TradeEvent {
                                seq: record.seq,
                                print_index: index as u32,
                                ts_socket: record.ts_socket,
                                ts_exchange: print.ts_exchange,
                                trade: print,
                            },
                            &mut ctx,
                        );
                        report.trade_events += 1;
                        report.events_emitted += 1;
                    }
                    continue;
                }
                _ => {
                    report.skipped_unparseable += 1;
                    continue;
                }
            }
        }

        if record.channel == Channel::BookTicker {
            match feed::top_of_book(venue, record.channel, &record.payload) {
                Some(top) => {
                    strategy.on_top_of_book(
                        &TopBookEvent {
                            seq: record.seq,
                            ts_socket: record.ts_socket,
                            ts_exchange: top.ts_exchange,
                            top,
                        },
                        &mut ctx,
                    );
                    report.top_book_events += 1;
                    report.events_emitted += 1;
                    continue;
                }
                None => {
                    report.skipped_unparseable += 1;
                    continue;
                }
            }
        }

        if let Some(snapshot) = feed::inband_snapshot(venue, record.channel, &record.payload) {
            strategy.on_snapshot(
                &SnapshotEvent {
                    seq: record.seq,
                    ts_socket: record.ts_socket,
                    snapshot,
                },
                &mut ctx,
            );
            report.events_emitted += 1;
            continue;
        }

        let (Some(span), Some(diff)) = (
            feed::update_span(venue, record.channel, &record.payload),
            feed::book_diff(venue, record.channel, &record.payload),
        ) else {
            report.skipped_unparseable += 1;
            continue;
        };

        strategy.on_book_diff(
            &BookDiffEvent {
                seq: record.seq,
                ts_socket: record.ts_socket,
                ts_exchange: record.ts_exchange,
                span,
                diff,
            },
            &mut ctx,
        );
        report.events_emitted += 1;
    }

    strategy.on_end(report.frames, &mut ctx);
    report.signals = ctx.signals.len() as u64;
    report.signal_hash = signal_hash(&ctx.signals);

    Ok(report)
}

pub fn signal_hash(signals: &[Signal]) -> String {
    let mut hasher = Sha256::new();
    for signal in signals {
        hasher.update(signal.tag.len().to_le_bytes());
        hasher.update(signal.tag.as_bytes());
        hasher.update(signal.payload.len().to_le_bytes());
        hasher.update(&signal.payload);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Render the replay report exactly as the CLI prints it.
///
/// Like the audit report, this format is load-bearing for anything that
/// parses `signal_hash` out of replay runs. Change it with a golden test.
pub fn format_report(input: &Path, seed: u64, report: &ReplayReport) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "input       {}", input.display());
    let _ = writeln!(out, "seed        {seed}");
    let _ = writeln!(out, "frames      {}", report.frames);
    let _ = writeln!(out, "events      {}", report.events_emitted);
    let _ = writeln!(out, "trades      {}", report.trade_events);
    let _ = writeln!(out, "topbooks    {}", report.top_book_events);
    let _ = writeln!(out, "gaps        {}", report.gaps);
    let _ = writeln!(
        out,
        "skipped     {}",
        report.skipped_channel + report.skipped_unparseable
    );
    let _ = writeln!(out, "signals     {}", report.signals);
    let _ = writeln!(out, "signal_hash {}", report.signal_hash);

    out
}

fn read_manifest(input: &Path) -> Result<CaptureManifest, ReplayError> {
    let body = std::fs::read_to_string(input.join(MANIFEST_FILE))?;
    let manifest: CaptureManifest = serde_json::from_str(&body)?;
    // A format change must fail loudly here, not misread silently.
    if manifest.schema_version != astra_types::SCHEMA_VERSION {
        return Err(ReplayError::Malformed(format!(
            "unsupported capture schema version {}, this build reads {}",
            manifest.schema_version,
            astra_types::SCHEMA_VERSION
        )));
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use astra_types::{CaptureId, CaptureRecord, Instrument, MarketType, Symbol, Venue};
    use rand::Rng;
    use std::path::PathBuf;

    const DEPTH_FRAME: &str = r#"{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":100,"u":105,"b":[["108105.12000000","0.00100000"]],"a":[["108105.13000000","0.00200000"]]}"#;
    const NEXT_FRAME: &str = r#"{"e":"depthUpdate","E":1700000000100,"s":"BTCUSDT","U":106,"u":110,"b":[["108105.11000000","0"]],"a":[]}"#;
    const BYBIT_SNAPSHOT: &str = r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["82994.80000000","0.444429"]],"a":[["82994.90000000","1.132658"]],"u":500,"seq":1}}"#;
    const BYBIT_DELTA: &str = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[],"a":[["83002.60000000","0.025997"]],"u":501,"seq":2}}"#;

    struct Recorder {
        diffs: u64,
        snapshots: u64,
        trades: u64,
        topbooks: u64,
        gaps: u64,
        ends: u64,
        use_rng: bool,
    }

    impl Recorder {
        fn plain() -> Self {
            Recorder {
                diffs: 0,
                snapshots: 0,
                trades: 0,
                topbooks: 0,
                gaps: 0,
                ends: 0,
                use_rng: false,
            }
        }
    }

    impl Strategy for Recorder {
        fn on_book_diff(&mut self, event: &BookDiffEvent, ctx: &mut Context) {
            self.diffs += 1;
            ctx.emit("diff", event.seq.to_le_bytes().to_vec());
            if self.use_rng {
                let byte: u8 = ctx.rng().random();
                ctx.emit("rand", vec![byte]);
            }
        }

        fn on_snapshot(&mut self, event: &SnapshotEvent, ctx: &mut Context) {
            self.snapshots += 1;
            ctx.emit("snapshot", event.seq.to_le_bytes().to_vec());
        }

        fn on_trade(&mut self, event: &TradeEvent, ctx: &mut Context) {
            self.trades += 1;
            let mut payload = Vec::with_capacity(20);
            payload.extend_from_slice(&event.seq.to_le_bytes());
            payload.extend_from_slice(&event.print_index.to_le_bytes());
            ctx.emit("trade", payload);
        }

        fn on_top_of_book(&mut self, event: &TopBookEvent, ctx: &mut Context) {
            self.topbooks += 1;
            ctx.emit("topbook", event.seq.to_le_bytes().to_vec());
        }

        fn on_gap(&mut self, marker: &GapMarker, ctx: &mut Context) {
            self.gaps += 1;
            ctx.emit("gap", marker.reason.as_bytes().to_vec());
        }

        fn on_end(&mut self, _frames: u64, _ctx: &mut Context) {
            self.ends += 1;
        }
    }

    fn instrument() -> Instrument {
        Instrument::new(
            Venue::Binance,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    fn temp_directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("astra-replay-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn record(seq: u64, instrument: &Instrument, payload: Vec<u8>) -> CaptureRecord {
        CaptureRecord {
            seq,
            instrument: instrument.clone(),
            channel: Channel::BookDiff,
            ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000 + seq as i64),
            ts_exchange: None,
            payload,
            flags: CaptureFlags::NONE,
        }
    }

    fn write_capture(output: &Path, instrument: &Instrument, payloads: Vec<Vec<u8>>) {
        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();

        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), 100).unwrap();
        for (index, payload) in payloads.iter().enumerate() {
            writer
                .append(&record(index as u64, instrument, payload.clone()))
                .unwrap();
        }
        writer.finish().unwrap();

        let manifest = CaptureManifest {
            schema_version: astra_types::SCHEMA_VERSION,
            capture_id: CaptureId::new("replay-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument.clone(),
            channel: Channel::BookDiff,
            frames_written: payloads.len() as u64,
            stop_reason: None,
        };
        astra_record::capture::write_manifest(output, &manifest).unwrap();
    }

    fn bybit_instrument() -> Instrument {
        Instrument::new(
            Venue::Bybit,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    #[test]
    fn replay_emits_events_and_hashes_signals() {
        let input = temp_directory("events");
        write_capture(
            &input,
            &instrument(),
            vec![
                DEPTH_FRAME.as_bytes().to_vec(),
                NEXT_FRAME.as_bytes().to_vec(),
            ],
        );

        let mut strategy = Recorder::plain();
        let report = replay(&input, 7, &mut strategy).unwrap();

        assert_eq!(report.frames, 2);
        assert_eq!(report.events_emitted, 2);
        assert_eq!(report.gaps, 0);
        assert_eq!(report.signals, 2);
        assert_eq!(strategy.diffs, 2);
        assert_eq!(strategy.ends, 1);
        assert_eq!(report.signal_hash.len(), 64);

        std::fs::remove_dir_all(&input).unwrap();
    }

    #[test]
    fn same_seed_replays_identically_and_other_seeds_differ() {
        let input = temp_directory("seeds");
        write_capture(&input, &instrument(), vec![DEPTH_FRAME.as_bytes().to_vec()]);

        let mut first = Recorder {
            use_rng: true,
            ..Recorder::plain()
        };
        let mut second = Recorder {
            use_rng: true,
            ..Recorder::plain()
        };
        let mut third = Recorder {
            use_rng: true,
            ..Recorder::plain()
        };
        let one = replay(&input, 42, &mut first).unwrap();
        let two = replay(&input, 42, &mut second).unwrap();
        let three = replay(&input, 99, &mut third).unwrap();

        assert_eq!(one.signal_hash, two.signal_hash);
        assert_ne!(one.signal_hash, three.signal_hash);

        let mut plain_a = Recorder::plain();
        let mut plain_b = Recorder::plain();
        let four = replay(&input, 1, &mut plain_a).unwrap();
        let five = replay(&input, 2, &mut plain_b).unwrap();
        assert_eq!(four.signal_hash, five.signal_hash);

        std::fs::remove_dir_all(&input).unwrap();
    }

    #[test]
    fn gap_markers_surface_as_gap_events() {
        let input = temp_directory("gaps");
        let marker = GapMarker {
            started_at: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
            ended_at: Timestamp::from_unix_nanos(1_700_000_000_100_000_000),
            attempts: 1,
            reason: "venue_close".to_owned(),
        };
        let mut gap = record(1, &instrument(), serde_json::to_vec(&marker).unwrap());
        gap.flags = CaptureFlags::SYNTHETIC
            .union(CaptureFlags::SEQUENCE_GAP)
            .union(CaptureFlags::UNRELIABLE);

        std::fs::create_dir_all(input.join(FRAMES_DIR)).unwrap();
        let mut writer = store::ChunkWriter::open(input.join(FRAMES_DIR), 100).unwrap();
        writer
            .append(&record(0, &instrument(), DEPTH_FRAME.as_bytes().to_vec()))
            .unwrap();
        writer.append(&gap).unwrap();
        writer.finish().unwrap();

        let manifest = CaptureManifest {
            schema_version: astra_types::SCHEMA_VERSION,
            capture_id: CaptureId::new("replay-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument(),
            channel: Channel::BookDiff,
            frames_written: 1,
            stop_reason: None,
        };
        astra_record::capture::write_manifest(&input, &manifest).unwrap();

        let mut strategy = Recorder::plain();
        let report = replay(&input, 7, &mut strategy).unwrap();

        assert_eq!(report.frames, 2);
        assert_eq!(report.gaps, 1);
        assert_eq!(strategy.gaps, 1);
        assert_eq!(strategy.diffs, 1);

        std::fs::remove_dir_all(&input).unwrap();
    }

    #[test]
    fn bybit_snapshots_arrive_as_snapshot_events() {
        let input = temp_directory("bybit-events");
        write_capture(
            &input,
            &bybit_instrument(),
            vec![
                BYBIT_SNAPSHOT.as_bytes().to_vec(),
                BYBIT_DELTA.as_bytes().to_vec(),
            ],
        );

        let mut strategy = Recorder::plain();
        let report = replay(&input, 7, &mut strategy).unwrap();

        assert_eq!(report.events_emitted, 2);
        assert_eq!(strategy.snapshots, 1);
        assert_eq!(strategy.diffs, 1);
        assert_eq!(report.gaps, 0);

        std::fs::remove_dir_all(&input).unwrap();
    }

    #[test]
    fn other_channels_and_garbage_are_counted_not_crashed() {
        let input = temp_directory("skipped");
        std::fs::create_dir_all(input.join(FRAMES_DIR)).unwrap();

        // A channel outside the replay set (funding is never replayed).
        let skipped = CaptureRecord {
            seq: 1,
            instrument: instrument(),
            channel: Channel::Funding,
            ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_001),
            ts_exchange: None,
            payload: b"{}".to_vec(),
            flags: CaptureFlags::NONE,
        };

        // A trade-channel frame that cannot be parsed.
        let bad_trade = CaptureRecord {
            seq: 2,
            instrument: instrument(),
            channel: Channel::Trade,
            ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_002),
            ts_exchange: None,
            payload: b"{}".to_vec(),
            flags: CaptureFlags::NONE,
        };

        let mut writer = store::ChunkWriter::open(input.join(FRAMES_DIR), 100).unwrap();
        writer
            .append(&record(0, &instrument(), DEPTH_FRAME.as_bytes().to_vec()))
            .unwrap();
        writer.append(&skipped).unwrap();
        writer.append(&bad_trade).unwrap();
        writer
            .append(&record(3, &instrument(), b"not json".to_vec()))
            .unwrap();
        writer.finish().unwrap();

        let manifest = CaptureManifest {
            schema_version: astra_types::SCHEMA_VERSION,
            capture_id: CaptureId::new("replay-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument(),
            channel: Channel::BookDiff,
            frames_written: 4,
            stop_reason: None,
        };
        astra_record::capture::write_manifest(&input, &manifest).unwrap();

        let mut strategy = Recorder::plain();
        let report = replay(&input, 7, &mut strategy).unwrap();

        assert_eq!(report.frames, 4);
        assert_eq!(report.events_emitted, 1);
        assert_eq!(report.skipped_channel, 1);
        assert_eq!(report.skipped_unparseable, 2);
        assert_eq!(strategy.diffs, 1);

        std::fs::remove_dir_all(&input).unwrap();
    }

    #[test]
    fn trade_frames_replay_with_bundle_expansion() {
        use astra_types::{MarketType, Symbol, Venue};

        let input = temp_directory("trade-events");
        let bybit = Instrument::new(
            Venue::Bybit,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        );
        let bundle = br#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672304486868,"data":[{"T":1672304486865,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"16578.50","i":"aaa","seq":1},{"T":1672304486866,"s":"BTCUSDT","S":"Sell","v":"0.002","p":"16578.51","i":"bbb","seq":2}]}"#;

        std::fs::create_dir_all(input.join(FRAMES_DIR)).unwrap();
        let mut writer = store::ChunkWriter::open(input.join(FRAMES_DIR), 100).unwrap();
        writer
            .append(&CaptureRecord {
                seq: 0,
                instrument: bybit.clone(),
                channel: Channel::Trade,
                ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
                ts_exchange: None,
                payload: bundle.to_vec(),
                flags: CaptureFlags::NONE,
            })
            .unwrap();
        writer.finish().unwrap();

        let manifest = CaptureManifest {
            schema_version: astra_types::SCHEMA_VERSION,
            capture_id: CaptureId::new("replay-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: bybit,
            channel: Channel::Trade,
            frames_written: 1,
            stop_reason: None,
        };
        astra_record::capture::write_manifest(&input, &manifest).unwrap();

        let mut strategy = Recorder::plain();
        let report = replay(&input, 7, &mut strategy).unwrap();

        assert_eq!(report.frames, 1);
        assert_eq!(report.trade_events, 2);
        assert_eq!(report.events_emitted, 2);
        assert_eq!(strategy.trades, 2);
        assert_eq!(report.signals, 2);

        std::fs::remove_dir_all(&input).unwrap();
    }

    #[test]
    fn top_of_book_frames_replay() {
        let input = temp_directory("topbook-events");
        let payload = include_str!("../../astra-record/testdata/binance_book_ticker.json");

        std::fs::create_dir_all(input.join(FRAMES_DIR)).unwrap();
        let mut writer = store::ChunkWriter::open(input.join(FRAMES_DIR), 100).unwrap();
        writer
            .append(&CaptureRecord {
                seq: 0,
                instrument: instrument(),
                channel: Channel::BookTicker,
                ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
                ts_exchange: None,
                payload: payload.as_bytes().to_vec(),
                flags: CaptureFlags::NONE,
            })
            .unwrap();
        writer.finish().unwrap();

        let manifest = CaptureManifest {
            schema_version: astra_types::SCHEMA_VERSION,
            capture_id: CaptureId::new("replay-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument(),
            channel: Channel::BookTicker,
            frames_written: 1,
            stop_reason: None,
        };
        astra_record::capture::write_manifest(&input, &manifest).unwrap();

        let mut strategy = Recorder::plain();
        let report = replay(&input, 7, &mut strategy).unwrap();

        assert_eq!(report.frames, 1);
        assert_eq!(report.top_book_events, 1);
        assert_eq!(report.events_emitted, 1);
        assert_eq!(strategy.topbooks, 1);

        std::fs::remove_dir_all(&input).unwrap();
    }

    #[test]
    fn a_torn_manifest_fails_loudly() {
        let input = temp_directory("torn");
        write_capture(&input, &instrument(), vec![DEPTH_FRAME.as_bytes().to_vec()]);

        let body = std::fs::read_to_string(input.join(MANIFEST_FILE)).unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(&body).unwrap();
        manifest["frames_written"] = serde_json::json!(99);
        std::fs::write(
            input.join(MANIFEST_FILE),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();

        let mut strategy = Recorder::plain();
        assert!(matches!(
            replay(&input, 7, &mut strategy),
            Err(ReplayError::Malformed(_))
        ));

        std::fs::remove_dir_all(&input).unwrap();
    }

    #[test]
    fn a_future_schema_version_is_refused_not_misread() {
        let input = temp_directory("schema-version");
        write_capture(&input, &instrument(), vec![DEPTH_FRAME.as_bytes().to_vec()]);

        let body = std::fs::read_to_string(input.join(MANIFEST_FILE)).unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(&body).unwrap();
        manifest["schema_version"] = serde_json::json!(astra_types::SCHEMA_VERSION + 1);
        std::fs::write(
            input.join(MANIFEST_FILE),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();

        let mut strategy = Recorder::plain();
        assert!(matches!(
            replay(&input, 7, &mut strategy),
            Err(ReplayError::Malformed(_))
        ));

        std::fs::remove_dir_all(&input).unwrap();
    }

    #[test]
    fn the_report_format_is_pinned_line_by_line() {
        let report = ReplayReport {
            frames: 96,
            events_emitted: 96,
            trade_events: 0,
            top_book_events: 0,
            gaps: 0,
            skipped_channel: 0,
            skipped_unparseable: 0,
            signals: 96,
            signal_hash: "f66ae25f".repeat(8),
        };
        let text = format_report(std::path::Path::new("./capture"), 7, &report);
        for line in [
            "input       ./capture",
            "seed        7",
            "frames      96",
            "events      96",
            "trades      0",
            "topbooks    0",
            "gaps        0",
            "skipped     0",
            "signals     96",
            "signal_hash f66ae25ff66ae25ff66ae25ff66ae25ff66ae25ff66ae25ff66ae25ff66ae25f",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }
    }
}
