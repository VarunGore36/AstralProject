use std::path::Path;

use astra_book::{BookError, BookSnapshot, OrderBook, Reconstructor, UpdateSpan};
use astra_types::{CaptureFlags, GapMarker};

use crate::capture::FRAMES_DIR;
use crate::error::RecordError;
use crate::feed;
use crate::store;

#[derive(Debug, Default)]
pub struct ReconstructionSummary {
    pub records: u64,
    pub venue_frames: u64,
    pub synthetic_records: u64,
    pub diffs_applied: u64,
    pub frames_without_a_book: u64,
    pub invalid_diffs: u64,
    pub skipped_before_snapshot: u64,
    pub gaps: u64,
    pub rejected_after_gap: u64,
    /// Connection-type gap markers (reconnects). Sequence-type gaps surface
    /// through the reconstructor's own discontinuity check instead, so they
    /// are not double-counted here. (A reconnect whose fresh spans also jump
    /// the venue sequence counts in both this and `gaps` — once for the
    /// capture event, once for the book impact. That overlap is honest;
    /// merging them would hide which happened.)
    pub connection_gaps: u64,
    /// Synthetic records no parser understands. Counted, not hidden.
    pub undecodable_gaps: u64,
    pub snapshot_loaded: Option<u64>,
    pub inband_snapshots: u64,
    pub first_error: Option<BookError>,
    pub book: OrderBook,
}

pub fn reconstruct(
    input: &Path,
    snapshot: Option<&Path>,
) -> Result<ReconstructionSummary, RecordError> {
    // Gated like every other reader: version-checked manifest first, so a
    // foreign or half-written capture fails here instead of rebuilding a
    // book out of bytes this build cannot understand.
    let manifest = crate::check::read_manifest(input)?;
    let mut summary = ReconstructionSummary::default();
    let mut reconstructor = Reconstructor::new();

    if let Some(path) = snapshot {
        let loaded = load_snapshot(path)?;
        reconstructor.load_snapshot(&loaded)?;
        summary.snapshot_loaded = Some(loaded.last_update_id);
    }

    for record in store::read_all(&input.join(FRAMES_DIR))? {
        summary.records += 1;

        if record.flags.contains(CaptureFlags::SYNTHETIC) {
            summary.synthetic_records += 1;
            match serde_json::from_slice::<GapMarker>(&record.payload) {
                Ok(marker) if marker.reason.starts_with("update_id_gap") => {}
                Ok(_) => summary.connection_gaps += 1,
                Err(_) => summary.undecodable_gaps += 1,
            }
            continue;
        }

        summary.venue_frames += 1;

        let venue = record.instrument.venue();
        if let Some(snapshot) = feed::inband_snapshot(venue, record.channel, &record.payload) {
            reconstructor.load_snapshot(&snapshot)?;
            summary.inband_snapshots += 1;
            continue;
        }

        let Some(diff) = feed::book_diff(venue, record.channel, &record.payload) else {
            summary.frames_without_a_book += 1;
            continue;
        };

        let Some(span) = feed::update_span(venue, record.channel, &record.payload) else {
            summary.frames_without_a_book += 1;
            continue;
        };

        apply(&mut reconstructor, &mut summary, span, &diff);
    }

    summary.skipped_before_snapshot = reconstructor.skipped_before_snapshot;
    summary.gaps = reconstructor.gaps;
    summary.rejected_after_gap = reconstructor.rejected_after_gap;
    summary.book = reconstructor.book().clone();

    if summary.venue_frames != manifest.frames_written {
        return Err(RecordError::ManifestMismatch {
            claimed: manifest.frames_written,
            actual: summary.venue_frames,
        });
    }

    Ok(summary)
}

fn apply(
    reconstructor: &mut Reconstructor,
    summary: &mut ReconstructionSummary,
    span: UpdateSpan,
    diff: &astra_book::BookDiff,
) {
    match reconstructor.apply_event(span, diff) {
        Ok(astra_book::ApplyDecision::Applied) => summary.diffs_applied += 1,
        Ok(astra_book::ApplyDecision::SkippedBeforeSnapshot) => {}
        Ok(astra_book::ApplyDecision::Discontinuity) => {}
        Err(error) => {
            summary.invalid_diffs += 1;
            summary.first_error.get_or_insert(error);
        }
    }
}

fn load_snapshot(path: &Path) -> Result<BookSnapshot, RecordError> {
    let payload = std::fs::read(path)?;

    feed::book_snapshot(
        astra_types::Venue::Binance,
        astra_types::Channel::BookSnapshot,
        &payload,
    )
    .ok_or_else(|| RecordError::Snapshot(path.display().to_string()))
}

/// Render the reconstruction summary exactly as the CLI prints it.
///
/// Golden-tested with the other report printers.
pub fn format_summary(
    input: &Path,
    snapshot: Option<&Path>,
    summary: &ReconstructionSummary,
) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "input       {}", input.display());
    let _ = writeln!(
        out,
        "snapshot    {}",
        snapshot
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| format!("in-band only ({})", summary.inband_snapshots))
    );
    let _ = writeln!(out, "records     {}", summary.records);
    let _ = writeln!(out, "venue       {}", summary.venue_frames);
    let _ = writeln!(out, "synthetic   {}", summary.synthetic_records);
    let _ = writeln!(out, "applied     {}", summary.diffs_applied);
    let _ = writeln!(out, "unchecked   {}", summary.frames_without_a_book);
    let _ = writeln!(out, "invalid     {}", summary.invalid_diffs);
    let _ = writeln!(out, "skipped     {}", summary.skipped_before_snapshot);
    let _ = writeln!(out, "inband      {}", summary.inband_snapshots);
    let _ = writeln!(out, "gaps        {}", summary.gaps);
    let _ = writeln!(out, "conn_gaps   {}", summary.connection_gaps);
    let _ = writeln!(out, "undecodable {}", summary.undecodable_gaps);
    let _ = writeln!(out, "rejected    {}", summary.rejected_after_gap);
    let _ = writeln!(out, "bid levels  {}", summary.book.bids_len());
    let _ = writeln!(out, "ask levels  {}", summary.book.asks_len());
    let _ = writeln!(
        out,
        "best bid    {}",
        describe_level(summary.book.best_bid())
    );
    let _ = writeln!(
        out,
        "best ask    {}",
        describe_level(summary.book.best_ask())
    );
    let _ = writeln!(out, "mid         {}", describe_fixed(summary.book.mid()));
    let _ = writeln!(out, "spread      {}", describe_fixed(summary.book.spread()));
    let _ = writeln!(out, "crossed     {}", summary.book.is_crossed());

    if summary.snapshot_loaded.is_none() && summary.inband_snapshots == 0 {
        let _ = writeln!(
            out,
            "note        the book is partial: no snapshot bootstrap"
        );
    }
    if let Some(error) = summary.first_error {
        let _ = writeln!(out, "first error {error}");
    }

    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use astra_types::{
        CaptureId, CaptureManifest, CaptureRecord, Channel, GapMarker, Instrument, MarketType,
        SCHEMA_VERSION, Symbol, Timestamp, Venue,
    };
    use std::path::PathBuf;

    const REAL_FRAME: &str = include_str!("../testdata/binance_depth_update.json");

    fn instrument() -> Instrument {
        Instrument::new(
            Venue::Binance,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    fn temp_directory(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("astra-reconstruct-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn write_capture(output: &Path, payloads: Vec<Vec<u8>>) {
        write_capture_as(output, payloads, instrument());
    }

    fn write_capture_as(output: &Path, payloads: Vec<Vec<u8>>, instrument: Instrument) {
        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();

        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), 100).unwrap();
        for (index, payload) in payloads.iter().enumerate() {
            writer
                .append(&CaptureRecord {
                    seq: index as u64,
                    instrument: instrument.clone(),
                    channel: Channel::BookDiff,
                    ts_socket: Timestamp::from_unix_nanos(index as i64),
                    ts_exchange: None,
                    payload: payload.clone(),
                    flags: CaptureFlags::NONE,
                })
                .unwrap();
        }
        writer.finish().unwrap();

        let manifest = CaptureManifest {
            schema_version: SCHEMA_VERSION,
            capture_id: CaptureId::new("reconstruct-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument,
            channel: Channel::BookDiff,
            frames_written: payloads.len() as u64,
            stop_reason: None,
        };
        crate::capture::write_manifest(output, &manifest).unwrap();
    }

    fn write_snapshot(path: &Path, body: &str) {
        std::fs::write(path, body).unwrap();
    }

    fn real_frame() -> Vec<u8> {
        REAL_FRAME.as_bytes().to_vec()
    }

    const REAL_SEQUENCE: &str = include_str!("../testdata/binance_depth_sequence.json");

    fn real_sequence() -> Vec<Vec<u8>> {
        let frames: Vec<serde_json::Value> = serde_json::from_str(REAL_SEQUENCE).unwrap();
        frames
            .into_iter()
            .map(|frame| serde_json::to_vec(&frame).unwrap())
            .collect()
    }

    #[test]
    fn eight_consecutive_real_frames_reconstruct_cleanly() {
        let output = temp_directory("sequence");
        let payloads = real_sequence();
        assert_eq!(payloads.len(), 8);
        write_capture(&output, payloads);

        let summary = reconstruct(&output, None).unwrap();

        assert_eq!(summary.records, 8);
        assert_eq!(summary.venue_frames, 8);
        assert_eq!(summary.diffs_applied, 8);
        assert_eq!(summary.frames_without_a_book, 0);
        assert_eq!(summary.invalid_diffs, 0);
        assert_eq!(summary.gaps, 0);
        assert!(!summary.book.is_empty());
        assert!(!summary.book.is_crossed());
        assert!(summary.book.spread().unwrap().raw() > 0);

        std::fs::remove_dir_all(&output).unwrap();
    }

    fn bybit_instrument() -> Instrument {
        Instrument::new(
            Venue::Bybit,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    fn bybit_sequence() -> Vec<Vec<u8>> {
        let payload: &str = include_str!("../testdata/bybit_snapshot_deltas.json");
        let frames: Vec<serde_json::Value> = serde_json::from_str(payload).unwrap();
        frames
            .into_iter()
            .map(|frame| serde_json::to_vec(&frame).unwrap())
            .collect()
    }

    #[test]
    fn a_bybit_snapshot_bootstraps_the_book_inband() {
        let output = temp_directory("bybit-snapshot");
        write_capture_as(&output, bybit_sequence(), bybit_instrument());

        let summary = reconstruct(&output, None).unwrap();

        assert_eq!(summary.records, 4);
        assert_eq!(summary.venue_frames, 4);
        assert_eq!(summary.inband_snapshots, 1);
        assert_eq!(summary.diffs_applied, 3);
        assert_eq!(summary.gaps, 0);
        assert_eq!(summary.rejected_after_gap, 0);
        assert_eq!(summary.book.bids_len(), 50);
        assert_eq!(summary.book.asks_len(), 50);
        assert!(!summary.book.is_crossed());

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_second_inband_snapshot_resyncs_the_book() {
        let output = temp_directory("bybit-resync");
        let mut payloads = bybit_sequence();
        let repeat: Vec<Vec<u8>> = payloads[0..2].to_vec();
        payloads.extend(repeat);
        write_capture_as(&output, payloads, bybit_instrument());

        let summary = reconstruct(&output, None).unwrap();

        assert_eq!(summary.records, 6);
        assert_eq!(summary.inband_snapshots, 2);
        assert_eq!(summary.diffs_applied, 4);
        assert_eq!(summary.gaps, 0);
        assert!(!summary.book.is_crossed());

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_real_captured_frame_produces_a_book() {
        let output = temp_directory("real");
        write_capture(&output, vec![real_frame()]);

        let summary = reconstruct(&output, None).unwrap();

        assert_eq!(summary.records, 1);
        assert_eq!(summary.venue_frames, 1);
        assert_eq!(summary.diffs_applied, 1);
        assert_eq!(summary.frames_without_a_book, 0);
        assert_eq!(summary.invalid_diffs, 0);
        assert_eq!(summary.book.bids_len(), 6);
        assert_eq!(summary.book.asks_len(), 4);
        assert_eq!(
            summary.book.best_bid().unwrap().price.to_string(),
            "84162.47000000"
        );
        assert_eq!(
            summary.book.best_ask().unwrap().price.to_string(),
            "84162.48000000"
        );

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_zero_quantity_in_real_data_removes_the_level() {
        let output = temp_directory("remove");
        write_capture(&output, vec![real_frame()]);

        let summary = reconstruct(&output, None).unwrap();
        let bid_prices: Vec<String> = summary
            .book
            .levels(astra_book::Side::Bid, 32)
            .iter()
            .map(|level| level.price.to_string())
            .collect();

        assert_eq!(bid_prices.len(), 6);
        assert!(!bid_prices.contains(&"84158.91000000".to_owned()));
        assert!(!bid_prices.contains(&"75746.00000000".to_owned()));
        assert!(bid_prices.contains(&"84158.90000000".to_owned()));

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn synthetic_records_never_reach_the_book() {
        let output = temp_directory("synthetic");
        write_capture(&output, vec![real_frame()]);

        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), 100).unwrap();
        writer
            .append(&CaptureRecord {
                seq: 1,
                instrument: instrument(),
                channel: Channel::BookDiff,
                ts_socket: Timestamp::from_unix_nanos(1),
                ts_exchange: None,
                payload: b"{\"b\":[[\"1.00000000\",\"9\"]],\"a\":[]}".to_vec(),
                flags: CaptureFlags::SYNTHETIC
                    .union(CaptureFlags::SEQUENCE_GAP)
                    .union(CaptureFlags::UNRELIABLE),
            })
            .unwrap();
        writer.finish().unwrap();

        let summary = reconstruct(&output, None).unwrap();

        assert_eq!(summary.records, 2);
        assert_eq!(summary.synthetic_records, 1);
        assert_eq!(summary.venue_frames, 1);
        assert_eq!(summary.book.bids_len(), 6);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn connection_gaps_are_counted_and_undecodable_ones_surfaced() {
        let output = temp_directory("gap-counts");
        write_capture(&output, vec![real_frame()]);

        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), 100).unwrap();
        let marker = GapMarker {
            started_at: Timestamp::from_unix_nanos(1),
            ended_at: Timestamp::from_unix_nanos(2),
            attempts: 1,
            reason: "venue_close".to_owned(),
        };
        writer
            .append(&CaptureRecord {
                seq: 1,
                instrument: instrument(),
                channel: Channel::BookDiff,
                ts_socket: Timestamp::from_unix_nanos(1),
                ts_exchange: None,
                payload: serde_json::to_vec(&marker).unwrap(),
                flags: CaptureFlags::SYNTHETIC
                    .union(CaptureFlags::SEQUENCE_GAP)
                    .union(CaptureFlags::UNRELIABLE),
            })
            .unwrap();
        writer
            .append(&CaptureRecord {
                seq: 2,
                instrument: instrument(),
                channel: Channel::BookDiff,
                ts_socket: Timestamp::from_unix_nanos(2),
                ts_exchange: None,
                payload: b"not a gap marker".to_vec(),
                flags: CaptureFlags::SYNTHETIC
                    .union(CaptureFlags::SEQUENCE_GAP)
                    .union(CaptureFlags::UNRELIABLE),
            })
            .unwrap();
        writer.finish().unwrap();

        let summary = reconstruct(&output, None).unwrap();

        assert_eq!(summary.synthetic_records, 2);
        assert_eq!(summary.connection_gaps, 1);
        assert_eq!(summary.undecodable_gaps, 1);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn the_summary_format_is_pinned_line_by_line() {
        let summary = ReconstructionSummary {
            records: 601,
            venue_frames: 601,
            diffs_applied: 601,
            ..ReconstructionSummary::default()
        };
        let text = format_summary(std::path::Path::new("./capture"), None, &summary);
        for line in [
            "input       ./capture",
            "snapshot    in-band only (0)",
            "records     601",
            "venue       601",
            "applied     601",
            "best bid    none",
            "best ask    none",
            "crossed     false",
            "note        the book is partial: no snapshot bootstrap",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }
    }

    #[test]
    fn a_doctored_manifest_fails_the_reconstruction() {
        let output = temp_directory("doctored");
        write_capture(&output, vec![real_frame(), real_frame()]);

        let manifest_path = output.join(crate::capture::MANIFEST_FILE);
        let body = std::fs::read_to_string(&manifest_path).unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(&body).unwrap();
        manifest["frames_written"] = serde_json::json!(99);
        std::fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        assert!(matches!(
            reconstruct(&output, None),
            Err(crate::error::RecordError::ManifestMismatch { .. })
        ));

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn frames_without_a_book_are_counted_not_guessed() {
        let output = temp_directory("unchecked");
        write_capture(&output, vec![real_frame(), b"not json".to_vec()]);

        let summary = reconstruct(&output, None).unwrap();

        assert_eq!(summary.venue_frames, 2);
        assert_eq!(summary.diffs_applied, 1);
        assert_eq!(summary.frames_without_a_book, 1);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_snapshot_makes_the_book_complete() {
        let output = temp_directory("snapshot");
        write_capture(&output, vec![real_frame()]);

        let snapshot_path = output.join("snapshot.json");
        write_snapshot(
            &snapshot_path,
            r#"{"lastUpdateId":100697441889,"bids":[["70000.00000000","1"]],"asks":[["90000.00000000","1"]]}"#,
        );

        let summary = reconstruct(&output, Some(&snapshot_path)).unwrap();

        assert_eq!(summary.snapshot_loaded, Some(100697441889));
        assert_eq!(summary.diffs_applied, 1);
        assert_eq!(summary.book.bids_len(), 7);
        assert_eq!(
            summary.book.best_bid().unwrap().price.to_string(),
            "84162.47000000"
        );

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn events_before_the_snapshot_are_skipped() {
        let output = temp_directory("skip");
        write_capture(&output, vec![real_frame()]);

        let snapshot_path = output.join("snapshot.json");
        write_snapshot(
            &snapshot_path,
            r#"{"lastUpdateId":100697442000,"bids":[["70000.00000000","1"]],"asks":[["90000.00000000","1"]]}"#,
        );

        let summary = reconstruct(&output, Some(&snapshot_path)).unwrap();

        assert_eq!(summary.diffs_applied, 0);
        assert_eq!(summary.skipped_before_snapshot, 1);
        assert_eq!(summary.book.bids_len(), 1);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn an_unreadable_snapshot_is_an_error_not_a_guess() {
        let output = temp_directory("bad-snapshot");
        write_capture(&output, vec![real_frame()]);

        let snapshot_path = output.join("snapshot.json");
        write_snapshot(&snapshot_path, "not json");

        let result = reconstruct(&output, Some(&snapshot_path));
        assert!(matches!(result, Err(RecordError::Snapshot(_))));

        std::fs::remove_dir_all(&output).unwrap();
    }
}
