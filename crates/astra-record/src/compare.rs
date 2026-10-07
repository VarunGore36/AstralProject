use std::path::Path;

use astra_book::{
    ApplyDecision, BookDiff, BookError, BookSnapshot, Reconstructor, UpdateSpan, compare_top_levels,
};
use astra_types::CaptureFlags;

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
    let events = load_events(input)?;
    let references = load_references(reference)?;

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

fn load_events(input: &Path) -> Result<Vec<(UpdateSpan, BookDiff)>, RecordError> {
    let mut events = Vec::new();

    for record in store::read_all(&input.join(FRAMES_DIR))? {
        if record.flags.contains(CaptureFlags::SYNTHETIC) {
            continue;
        }
        let (Some(span), Some(diff)) = (
            feed::update_span(record.instrument.venue(), record.channel, &record.payload),
            feed::book_diff(record.instrument.venue(), record.channel, &record.payload),
        ) else {
            continue;
        };
        events.push((span, diff));
    }

    Ok(events)
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

fn load_references(reference: &Path) -> Result<Vec<BookSnapshot>, RecordError> {
    let mut snapshots = Vec::new();

    for record in store::read_all(&reference.join(FRAMES_DIR))? {
        if record.flags.contains(CaptureFlags::SYNTHETIC) {
            continue;
        }
        if let Some(snapshot) =
            feed::book_snapshot(record.instrument.venue(), record.channel, &record.payload)
        {
            snapshots.push(snapshot);
        }
    }

    snapshots.sort_by_key(|snapshot| snapshot.last_update_id);

    Ok(snapshots)
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
