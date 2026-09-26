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
}

pub fn compare(
    input: &Path,
    reference: &Path,
    levels: usize,
) -> Result<ComparisonReport, RecordError> {
    let events = load_events(input)?;
    let references = load_references(reference)?;

    Ok(compare_streams(&events, &references, levels)?)
}

pub fn compare_streams(
    events: &[(UpdateSpan, BookDiff)],
    references: &[BookSnapshot],
    levels: usize,
) -> Result<ComparisonReport, BookError> {
    let mut report = ComparisonReport {
        events: events.len() as u64,
        ..ComparisonReport::default()
    };

    let Some((bootstrap, checks)) = references.split_first() else {
        return Ok(report);
    };

    let mut reconstructor = Reconstructor::new();
    reconstructor.load_snapshot(bootstrap)?;
    report.bootstrap_frames = 1;

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
            report.first_mismatch.get_or_insert(mismatches.join("; "));
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

        let report = compare_streams(&events, &references, 2).unwrap();

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

        let report = compare_streams(&events, &references, 2).unwrap();

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

        let report = compare_streams(&events, &references, 4).unwrap();

        assert_eq!(report.mismatched, 1);
        assert!(report.first_mismatch.unwrap().contains("bid depth"));
    }

    #[test]
    fn no_references_means_no_checks() {
        let report = compare_streams(&[], &[], 10).unwrap();
        assert_eq!(report.checked, 0);
        assert_eq!(report.bootstrap_frames, 0);
    }
}
