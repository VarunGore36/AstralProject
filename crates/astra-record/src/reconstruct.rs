use std::path::Path;

use astra_book::{BookError, OrderBook};
use astra_types::CaptureFlags;

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
    pub first_error: Option<BookError>,
    pub book: OrderBook,
}

pub fn reconstruct(input: &Path) -> Result<ReconstructionSummary, RecordError> {
    let records = store::read_all(&input.join(FRAMES_DIR))?;
    let mut summary = ReconstructionSummary::default();

    for record in &records {
        summary.records += 1;

        if record.flags.contains(CaptureFlags::SYNTHETIC) {
            summary.synthetic_records += 1;
            continue;
        }

        summary.venue_frames += 1;

        let Some(diff) =
            feed::book_diff(record.instrument.venue(), record.channel, &record.payload)
        else {
            summary.frames_without_a_book += 1;
            continue;
        };

        match summary.book.apply_diff(&diff) {
            Ok(()) => summary.diffs_applied += 1,
            Err(error) => {
                summary.invalid_diffs += 1;
                summary.first_error.get_or_insert(error);
            }
        }
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use astra_types::{
        CaptureId, CaptureManifest, CaptureRecord, Channel, Instrument, MarketType, SCHEMA_VERSION,
        Symbol, Timestamp, Venue,
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

    fn write_capture(output: &Path, payloads: Vec<(&str, Vec<u8>)>) {
        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();

        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), 100).unwrap();
        for (index, (_label, payload)) in payloads.iter().enumerate() {
            writer
                .append(&CaptureRecord {
                    seq: index as u64,
                    instrument: instrument(),
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
            instrument: instrument(),
            channel: Channel::BookDiff,
            frames_written: payloads.len() as u64,
            stop_reason: None,
        };
        crate::capture::write_manifest(output, &manifest).unwrap();
    }

    #[test]
    fn a_real_captured_frame_produces_a_book() {
        let output = temp_directory("real");
        write_capture(&output, vec![("real", REAL_FRAME.as_bytes().to_vec())]);

        let summary = reconstruct(&output).unwrap();

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
        write_capture(&output, vec![("real", REAL_FRAME.as_bytes().to_vec())]);

        let summary = reconstruct(&output).unwrap();
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
        assert!(bid_prices.contains(&"84162.47000000".to_owned()));

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn synthetic_records_never_reach_the_book() {
        let output = temp_directory("synthetic");
        write_capture(&output, vec![("real", REAL_FRAME.as_bytes().to_vec())]);

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

        let summary = reconstruct(&output).unwrap();

        assert_eq!(summary.records, 2);
        assert_eq!(summary.synthetic_records, 1);
        assert_eq!(summary.venue_frames, 1);
        assert_eq!(summary.book.bids_len(), 6);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn frames_without_a_book_are_counted_not_guessed() {
        let output = temp_directory("unchecked");
        write_capture(
            &output,
            vec![
                ("real", REAL_FRAME.as_bytes().to_vec()),
                ("unknown", b"not json".to_vec()),
            ],
        );

        let summary = reconstruct(&output).unwrap();

        assert_eq!(summary.venue_frames, 2);
        assert_eq!(summary.diffs_applied, 1);
        assert_eq!(summary.frames_without_a_book, 1);

        std::fs::remove_dir_all(&output).unwrap();
    }
}
