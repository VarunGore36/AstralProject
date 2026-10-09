use std::path::Path;

use astra_book::{
    ApplyDecision, BookDiff, BookError, BookSnapshot, Reconstructor, UpdateSpan, compare_top_levels,
};
use astra_types::{CaptureFlags, GapMarker};

use crate::capture::FRAMES_DIR;
use crate::error::RecordError;
use crate::feed;
use crate::store;

#[derive(Debug, Default)]
pub struct ComparisonReport {
    pub events: u64,
    pub events_applied: u64,
    pub events_skipped: u64,
    pub events_rejected: u64,
    pub bootstrap_frames: u64,
    pub checked: u64,
    pub matched: u64,
    pub mismatched: u64,
    pub first_mismatch: Option<String>,
    pub mismatches: Vec<String>,
}

pub fn compare(
    input: &Path,
    reference: &Path,
    levels: usize,
    snapshot: Option<&Path>,
) -> Result<ComparisonReport, RecordError> {
    // Gated like every other reader: a torn manifest must fail here, not
    // produce a comparison against half a dataset.
    let manifest = crate::check::read_manifest(input)?;
    let reference_manifest = crate::check::read_manifest(reference)?;
    let (events, venue_frames) = load_events(input)?;
    if venue_frames != manifest.frames_written {
        return Err(RecordError::ManifestMismatch {
            claimed: manifest.frames_written,
            actual: venue_frames,
        });
    }
    let (references, reference_frames) = load_references(reference)?;
    if reference_frames != reference_manifest.frames_written {
        return Err(RecordError::ManifestMismatch {
            claimed: reference_manifest.frames_written,
            actual: reference_frames,
        });
    }

    let bootstrap = match snapshot {
        Some(path) => Some(load_snapshot(path)?),
        None => None,
    };

    Ok(compare_streams(
        &events,
        &references,
        levels,
        bootstrap.as_ref(),
    )?)
}

pub fn compare_streams(
    events: &[(UpdateSpan, BookDiff)],
    references: &[BookSnapshot],
    levels: usize,
    bootstrap: Option<&BookSnapshot>,
) -> Result<ComparisonReport, BookError> {
    let mut report = ComparisonReport {
        events: events.len() as u64,
        ..ComparisonReport::default()
    };

    let mut reconstructor = Reconstructor::new();

    let checks: &[BookSnapshot] = match bootstrap {
        Some(snapshot) => {
            reconstructor.load_snapshot(snapshot)?;
            report.bootstrap_frames = 1;
            references
        }
        None => {
            let Some((bootstrap, checks)) = references.split_first() else {
                return Ok(report);
            };
            reconstructor.load_snapshot(bootstrap)?;
            report.bootstrap_frames = 1;
            checks
        }
    };

    let mut index = 0;

    for snapshot in checks {
        while index < events.len() && events[index].0.last <= snapshot.last_update_id {
            let (span, diff) = &events[index];
            index += 1;

            match reconstructor.apply_event(*span, diff)? {
                ApplyDecision::Applied => report.events_applied += 1,
                ApplyDecision::SkippedBeforeSnapshot => report.events_skipped += 1,
                ApplyDecision::Discontinuity => report.events_rejected += 1,
            }
        }

        let mismatches = compare_top_levels(reconstructor.book(), snapshot, levels);
        report.checked += 1;

        if mismatches.is_empty() {
            report.matched += 1;
        } else {
            report.mismatched += 1;
            let book_bids =
                format_levels(&reconstructor.book().levels(astra_book::Side::Bid, levels));
            let mut ref_bids = snapshot.bids.clone();
            ref_bids.sort_by_key(|level| std::cmp::Reverse(level.price));
            ref_bids.truncate(levels);
            let detail = format!(
                "update {}: {} | book[{}] ref[{}]",
                snapshot.last_update_id,
                mismatches.join("; "),
                book_bids,
                format_levels(&ref_bids)
            );
            report.first_mismatch.get_or_insert(detail.clone());
            if report.mismatches.len() < 50 {
                report.mismatches.push(detail);
            }
        }
    }

    Ok(report)
}

fn load_events(input: &Path) -> Result<(Vec<(UpdateSpan, BookDiff)>, u64), RecordError> {
    let mut events = Vec::new();
    let mut venue_frames = 0u64;

    for record in store::read_all(&input.join(FRAMES_DIR))? {
        if record.flags.contains(CaptureFlags::SYNTHETIC) {
            // A gap marker no parser understands hides the shape of its hole;
            // fail loud like every other processor instead of skipping blind.
            if serde_json::from_slice::<GapMarker>(&record.payload).is_err() {
                return Err(crate::error::RecordError::UndecodableGap { seq: record.seq });
            }
            continue;
        }
        venue_frames += 1;
        let (Some(span), Some(diff)) = (
            feed::update_span(record.instrument.venue(), record.channel, &record.payload),
            feed::book_diff(record.instrument.venue(), record.channel, &record.payload),
        ) else {
            continue;
        };
        events.push((span, diff));
    }

    Ok((events, venue_frames))
}

fn format_levels(levels: &[astra_book::Level]) -> String {
    levels
        .iter()
        .map(|level| format!("{}x{}", level.price, level.quantity))
        .collect::<Vec<_>>()
        .join(" ")
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

fn load_references(reference: &Path) -> Result<(Vec<BookSnapshot>, u64), RecordError> {
    let mut snapshots = Vec::new();
    let mut venue_frames = 0u64;

    for record in store::read_all(&reference.join(FRAMES_DIR))? {
        if record.flags.contains(CaptureFlags::SYNTHETIC) {
            if serde_json::from_slice::<GapMarker>(&record.payload).is_err() {
                return Err(crate::error::RecordError::UndecodableGap { seq: record.seq });
            }
            continue;
        }
        venue_frames += 1;
        if let Some(snapshot) =
            feed::book_snapshot(record.instrument.venue(), record.channel, &record.payload)
        {
            snapshots.push(snapshot);
        }
    }

    snapshots.sort_by_key(|snapshot| snapshot.last_update_id);

    Ok((snapshots, venue_frames))
}

/// Render the comparison report exactly as the CLI prints it.
///
/// Golden-tested with the other report printers.
pub fn format_report(
    input: &Path,
    reference: &Path,
    levels: usize,
    verbose: bool,
    report: &ComparisonReport,
) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "input       {}", input.display());
    let _ = writeln!(out, "reference   {}", reference.display());
    let _ = writeln!(out, "levels      {levels}");
    let _ = writeln!(out, "events      {}", report.events);
    let _ = writeln!(out, "applied     {}", report.events_applied);
    let _ = writeln!(out, "skipped     {}", report.events_skipped);
    let _ = writeln!(out, "rejected    {}", report.events_rejected);
    let _ = writeln!(out, "bootstrap   {}", report.bootstrap_frames);
    let _ = writeln!(out, "checks      {}", report.checked);
    let _ = writeln!(out, "matched     {}", report.matched);
    let _ = writeln!(out, "mismatched  {}", report.mismatched);

    if let Some(mismatch) = &report.first_mismatch {
        let _ = writeln!(out, "first       {mismatch}");
    }

    if verbose {
        for mismatch in &report.mismatches {
            let _ = writeln!(out, "mismatch    {mismatch}");
        }
    }

    if report.checked == 0 {
        let _ = writeln!(
            out,
            "note        nothing was checked, so nothing is verified"
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use astra_book::Level;

    fn level(price: &str, quantity: &str) -> Level {
        Level {
            price: price.parse().unwrap(),
            quantity: quantity.parse().unwrap(),
        }
    }

    fn snapshot(last_update_id: u64, bids: Vec<Level>, asks: Vec<Level>) -> BookSnapshot {
        BookSnapshot {
            last_update_id,
            bids,
            asks,
        }
    }

    fn event(first: u64, last: u64, bids: Vec<Level>, asks: Vec<Level>) -> (UpdateSpan, BookDiff) {
        (UpdateSpan::new(first, last), BookDiff { bids, asks })
    }

    fn base_book() -> (Vec<Level>, Vec<Level>) {
        (
            vec![level("100.00000000", "1"), level("99.00000000", "2")],
            vec![level("101.00000000", "1"), level("102.00000000", "3")],
        )
    }

    #[test]
    fn a_correct_reconstruction_matches_every_reference() {
        let (bids, asks) = base_book();
        let references = vec![
            snapshot(100, bids.clone(), asks.clone()),
            snapshot(110, bids.clone(), asks.clone()),
            snapshot(
                120,
                bids,
                vec![level("101.00000000", "5"), level("102.00000000", "3")],
            ),
        ];
        let events = vec![
            event(101, 105, vec![level("98.00000000", "1")], vec![]),
            event(106, 115, vec![], vec![level("101.00000000", "5")]),
        ];

        let report = compare_streams(&events, &references, 2, None).unwrap();

        assert_eq!(report.bootstrap_frames, 1);
        assert_eq!(report.checked, 2);
        assert_eq!(report.matched, 2);
        assert_eq!(report.mismatched, 0);
        assert_eq!(report.events_applied, 2);
    }

    #[test]
    fn a_drifted_book_is_reported_not_hidden() {
        let (bids, asks) = base_book();
        let references = vec![
            snapshot(100, bids.clone(), asks.clone()),
            snapshot(
                110,
                vec![level("100.50000000", "9"), level("99.00000000", "2")],
                asks,
            ),
        ];
        let events = vec![event(101, 105, vec![], vec![])];

        let report = compare_streams(&events, &references, 2, None).unwrap();

        assert_eq!(report.checked, 1);
        assert_eq!(report.mismatched, 1);
        assert!(report.first_mismatch.unwrap().contains("bid level 0"));
    }

    #[test]
    fn a_depth_difference_is_reported() {
        let (bids, asks) = base_book();
        let references = vec![
            snapshot(100, bids.clone(), asks.clone()),
            snapshot(110, vec![level("100.00000000", "1")], asks),
        ];
        let events = Vec::new();

        let report = compare_streams(&events, &references, 4, None).unwrap();

        assert_eq!(report.mismatched, 1);
        assert!(report.first_mismatch.unwrap().contains("bid depth"));
    }

    #[test]
    fn an_external_bootstrap_checks_every_reference() {
        let (bids, asks) = base_book();
        let references = vec![
            snapshot(100, bids.clone(), asks.clone()),
            snapshot(110, bids.clone(), asks.clone()),
        ];
        let events = vec![event(91, 95, vec![level("98.00000000", "1")], vec![])];
        let external = snapshot(90, bids, asks);

        let report = compare_streams(&events, &references, 2, Some(&external)).unwrap();

        assert_eq!(report.bootstrap_frames, 1);
        assert_eq!(report.checked, 2);
        assert_eq!(report.matched, 2);
        assert_eq!(report.mismatched, 0);
        assert_eq!(report.events_applied, 1);
    }

    #[test]
    fn no_references_means_no_checks() {
        let report = compare_streams(&[], &[], 10, None).unwrap();
        assert_eq!(report.checked, 0);
        assert_eq!(report.bootstrap_frames, 0);
    }

    #[test]
    fn doctored_manifests_fail_the_comparison() {
        let input =
            std::env::temp_dir().join(format!("astra-compare-input-{}", std::process::id()));
        let reference =
            std::env::temp_dir().join(format!("astra-compare-reference-{}", std::process::id()));
        for dir in [&input, &reference] {
            let _ = std::fs::remove_dir_all(dir);
        }
        let instrument = astra_types::Instrument::new(
            astra_types::Venue::Binance,
            astra_types::MarketType::Spot,
            astra_types::Symbol::new("BTC/USDT").unwrap(),
        );
        let write = |dir: &std::path::Path,
                     channel: astra_types::Channel,
                     payload: &[u8]|
         -> Result<(), crate::error::RecordError> {
            std::fs::create_dir_all(dir.join(crate::capture::FRAMES_DIR))?;
            let mut writer =
                crate::store::ChunkWriter::open(dir.join(crate::capture::FRAMES_DIR), 100)?;
            writer.append(&astra_types::CaptureRecord {
                seq: 0,
                instrument: instrument.clone(),
                channel,
                ts_socket: astra_types::Timestamp::from_unix_nanos(0),
                ts_exchange: None,
                payload: payload.to_vec(),
                flags: astra_types::CaptureFlags::NONE,
            })?;
            writer.finish()?;
            crate::capture::write_manifest(
                dir,
                &astra_types::CaptureManifest {
                    schema_version: astra_types::SCHEMA_VERSION,
                    capture_id: astra_types::CaptureId::new("compare-test"),
                    created_at: astra_types::Timestamp::from_unix_nanos(0),
                    instrument: instrument.clone(),
                    channel,
                    frames_written: 1,
                    stop_reason: None,
                },
            )?;
            Ok(())
        };
        write(
            &input,
            astra_types::Channel::BookDiff,
            br#"{"e":"depthUpdate","s":"BTCUSDT","U":100,"u":105,"b":[],"a":[]}"#,
        )
        .unwrap();
        write(
            &reference,
            astra_types::Channel::BookSnapshot,
            br#"{"lastUpdateId":200,"bids":[],"asks":[]}"#,
        )
        .unwrap();

        assert!(compare(&input, &reference, 10, None).is_ok());

        for dir in [&input, &reference] {
            let path = dir.join(crate::capture::MANIFEST_FILE);
            let body = std::fs::read_to_string(&path).unwrap();
            let mut manifest: serde_json::Value = serde_json::from_str(&body).unwrap();
            manifest["frames_written"] = serde_json::json!(99);
            std::fs::write(&path, serde_json::to_string(&manifest).unwrap()).unwrap();

            assert!(matches!(
                compare(&input, &reference, 10, None),
                Err(crate::error::RecordError::ManifestMismatch { .. })
            ));

            // Restore before tampering the other side.
            manifest["frames_written"] = serde_json::json!(1);
            std::fs::write(&path, serde_json::to_string(&manifest).unwrap()).unwrap();
        }

        let _ = std::fs::remove_dir_all(&input);
        let _ = std::fs::remove_dir_all(&reference);
    }

    #[test]
    fn undecodable_gap_markers_abort_the_comparison() {
        let input =
            std::env::temp_dir().join(format!("astra-compare-poison-input-{}", std::process::id()));
        let reference =
            std::env::temp_dir().join(format!("astra-compare-poison-ref-{}", std::process::id()));
        for dir in [&input, &reference] {
            let _ = std::fs::remove_dir_all(dir);
        }
        let instrument = astra_types::Instrument::new(
            astra_types::Venue::Binance,
            astra_types::MarketType::Spot,
            astra_types::Symbol::new("BTC/USDT").unwrap(),
        );
        let venue_record =
            |seq: u64, channel: astra_types::Channel, payload: &[u8]| astra_types::CaptureRecord {
                seq,
                instrument: instrument.clone(),
                channel,
                ts_socket: astra_types::Timestamp::from_unix_nanos(seq as i64),
                ts_exchange: None,
                payload: payload.to_vec(),
                flags: astra_types::CaptureFlags::NONE,
            };
        let gap_record = |seq: u64, payload: Vec<u8>| astra_types::CaptureRecord {
            seq,
            instrument: instrument.clone(),
            channel: astra_types::Channel::BookDiff,
            ts_socket: astra_types::Timestamp::from_unix_nanos(seq as i64),
            ts_exchange: None,
            payload,
            flags: astra_types::CaptureFlags::SYNTHETIC
                .union(astra_types::CaptureFlags::SEQUENCE_GAP)
                .union(astra_types::CaptureFlags::UNRELIABLE),
        };
        let write_all = |dir: &std::path::Path, records: Vec<astra_types::CaptureRecord>| {
            std::fs::create_dir_all(dir.join(crate::capture::FRAMES_DIR)).unwrap();
            let mut writer =
                crate::store::ChunkWriter::open(dir.join(crate::capture::FRAMES_DIR), 100).unwrap();
            for record in &records {
                writer.append(record).unwrap();
            }
            writer.finish().unwrap();
            let venue_frames = records
                .iter()
                .filter(|record| !record.flags.contains(astra_types::CaptureFlags::SYNTHETIC))
                .count() as u64;
            crate::capture::write_manifest(
                dir,
                &astra_types::CaptureManifest {
                    schema_version: astra_types::SCHEMA_VERSION,
                    capture_id: astra_types::CaptureId::new("compare-test"),
                    created_at: astra_types::Timestamp::from_unix_nanos(0),
                    instrument: instrument.clone(),
                    channel: astra_types::Channel::BookDiff,
                    frames_written: venue_frames,
                    stop_reason: None,
                },
            )
            .unwrap();
        };
        let marker = serde_json::to_vec(&astra_types::GapMarker {
            started_at: astra_types::Timestamp::from_unix_nanos(1),
            ended_at: astra_types::Timestamp::from_unix_nanos(2),
            attempts: 0,
            reason: "venue_close".to_owned(),
        })
        .unwrap();
        write_all(
            &input,
            vec![
                venue_record(
                    0,
                    astra_types::Channel::BookDiff,
                    br#"{"e":"depthUpdate","s":"BTCUSDT","U":100,"u":105,"b":[],"a":[]}"#,
                ),
                gap_record(1, marker),
            ],
        );
        write_all(
            &reference,
            vec![venue_record(
                0,
                astra_types::Channel::BookSnapshot,
                br#"{"lastUpdateId":200,"bids":[],"asks":[]}"#,
            )],
        );
        // A decodable marker passes through silently on a clean run.
        assert!(compare(&input, &reference, 10, None).is_ok());

        // Poison the reference side with an undecodable marker: appending a
        // synthetic record leaves the venue count (and manifest) untouched.
        let mut writer =
            crate::store::ChunkWriter::open(reference.join(crate::capture::FRAMES_DIR), 100)
                .unwrap();
        writer
            .append(&gap_record(1, b"not a gap marker".to_vec()))
            .unwrap();
        writer.finish().unwrap();

        assert!(matches!(
            compare(&input, &reference, 10, None),
            Err(crate::error::RecordError::UndecodableGap { seq: 1 })
        ));

        let _ = std::fs::remove_dir_all(&input);
        let _ = std::fs::remove_dir_all(&reference);
    }

    #[test]
    fn the_report_format_is_pinned_line_by_line() {
        let report = ComparisonReport {
            events: 300,
            events_applied: 300,
            bootstrap_frames: 1,
            checked: 150,
            matched: 149,
            mismatched: 1,
            first_mismatch: Some("update 99: bid level 0".to_owned()),
            mismatches: vec!["update 99: bid level 0".to_owned()],
            ..ComparisonReport::default()
        };
        let text = format_report(
            std::path::Path::new("./capture"),
            std::path::Path::new("./reference"),
            10,
            true,
            &report,
        );
        for line in [
            "input       ./capture",
            "reference   ./reference",
            "levels      10",
            "events      300",
            "applied     300",
            "checks      150",
            "matched     149",
            "mismatched  1",
            "first       update 99: bid level 0",
            "mismatch    update 99: bid level 0",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }
        assert!(!text.contains("nothing was checked"), "{text}");

        let empty = ComparisonReport::default();
        let text = format_report(
            std::path::Path::new("./capture"),
            std::path::Path::new("./reference"),
            10,
            false,
            &empty,
        );
        assert!(
            text.contains("note        nothing was checked, so nothing is verified"),
            "{text}"
        );
    }
}
