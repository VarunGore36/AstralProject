use std::path::Path;

use astra_types::{CaptureFlags, CaptureManifest, GapMarker, Timestamp};

use crate::capture::{FRAMES_DIR, MANIFEST_FILE};
use crate::error::RecordError;
use crate::feed;
use crate::store;

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct SeqBreak {
    pub expected: u64,
    pub found: u64,
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct UpdateIdGap {
    pub expected: u64,
    pub found: u64,
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct GapDetail {
    pub seq: u64,
    pub attempts: u32,
    pub reason: String,
}

#[derive(Debug, Default)]
pub struct CheckReport {
    pub chunks: u64,
    pub records: u64,
    pub venue_frames: u64,
    pub synthetic_records: u64,
    pub checked_frames: u64,
    pub unchecked_frames: u64,
    pub connection_gaps: u64,
    pub update_id_gaps: Vec<UpdateIdGap>,
    pub seq_breaks: Vec<SeqBreak>,
    pub gap_details: Vec<GapDetail>,
    pub undecodable_gaps: u64,
    pub first_ts: Option<Timestamp>,
    pub last_ts: Option<Timestamp>,
    pub manifest_frames: Option<u64>,
}

impl CheckReport {
    pub fn is_healthy(&self) -> bool {
        self.seq_breaks.is_empty() && self.update_id_gaps.is_empty() && self.undecodable_gaps == 0
    }
}

pub fn check(input: &Path) -> Result<CheckReport, RecordError> {
    let manifest = read_manifest(input)?;
    let frames_dir = input.join(FRAMES_DIR);
    let index = store::read_index(&frames_dir)?;

    let mut report = CheckReport {
        manifest_frames: Some(manifest.frames_written),
        ..CheckReport::default()
    };

    let mut expected_seq = 0u64;
    let mut previous_last: Option<u64> = None;

    for entry in &index {
        let path = frames_dir.join(&entry.name);
        store::verify_chunk(&path, &entry.sha256)?;
        let records = store::read_chunk(&path)?;
        report.chunks += 1;

        for record in &records {
            report.records += 1;
            observe_timestamp(&mut report, record.ts_socket);

            if record.seq != expected_seq {
                report.seq_breaks.push(SeqBreak {
                    expected: expected_seq,
                    found: record.seq,
                });
                expected_seq = record.seq + 1;
            } else {
                expected_seq += 1;
            }

            if record.flags.contains(CaptureFlags::SYNTHETIC) {
                report.synthetic_records += 1;
                match serde_json::from_slice::<GapMarker>(&record.payload) {
                    Ok(marker) => {
                        if marker.reason.starts_with("update_id_gap") {
                            report.update_id_gaps.push(UpdateIdGap {
                                expected: 0,
                                found: 0,
                            });
                        } else {
                            report.connection_gaps += 1;
                        }
                        report.gap_details.push(GapDetail {
                            seq: record.seq,
                            attempts: marker.attempts,
                            reason: marker.reason,
                        });
                    }
                    Err(_) => report.undecodable_gaps += 1,
                }
                continue;
            }

            report.venue_frames += 1;

            match feed::update_span(record.instrument.venue(), record.channel, &record.payload) {
                Some(span) => {
                    report.checked_frames += 1;
                    if let Some(previous) = previous_last {
                        if span.first > previous + 1 {
                            report.update_id_gaps.push(UpdateIdGap {
                                expected: previous + 1,
                                found: span.first,
                            });
                        }
                    }
                    previous_last = Some(span.last);
                }
                None => report.unchecked_frames += 1,
            }
        }
    }

    Ok(report)
}

fn observe_timestamp(report: &mut CheckReport, timestamp: Timestamp) {
    if report.first_ts.is_none() {
        report.first_ts = Some(timestamp);
    }
    report.last_ts = Some(timestamp);
}

fn read_manifest(input: &Path) -> Result<CaptureManifest, RecordError> {
    let body = std::fs::read_to_string(input.join(MANIFEST_FILE))?;
    Ok(serde_json::from_str(&body)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use astra_types::{
        CaptureId, CaptureRecord, Channel, Instrument, MarketType, SCHEMA_VERSION, Symbol, Venue,
    };
    use std::path::PathBuf;

    fn instrument() -> Instrument {
        Instrument::new(
            Venue::Binance,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    fn temp_directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("astra-check-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn depth_frame(first: u64, last: u64) -> Vec<u8> {
        format!(r#"{{"e":"depthUpdate","s":"BTCUSDT","U":{first},"u":{last},"b":[],"a":[]}}"#)
            .into_bytes()
    }

    fn write_records(output: &Path, payloads: Vec<(Vec<u8>, CaptureFlags)>, chunk_size: usize) {
        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();

        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), chunk_size).unwrap();
        for (index, (payload, flags)) in payloads.iter().enumerate() {
            writer
                .append(&CaptureRecord {
                    seq: index as u64,
                    instrument: instrument(),
                    channel: Channel::BookDiff,
                    ts_socket: Timestamp::from_unix_nanos(index as i64),
                    ts_exchange: None,
                    payload: payload.clone(),
                    flags: *flags,
                })
                .unwrap();
        }
        writer.finish().unwrap();

        let manifest = CaptureManifest {
            schema_version: SCHEMA_VERSION,
            capture_id: CaptureId::new("check-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument(),
            channel: Channel::BookDiff,
            frames_written: payloads.len() as u64,
            stop_reason: None,
        };
        crate::capture::write_manifest(output, &manifest).unwrap();
    }

    fn gap_payload(reason: &str) -> Vec<u8> {
        serde_json::to_vec(&GapMarker {
            started_at: Timestamp::from_unix_nanos(1),
            ended_at: Timestamp::from_unix_nanos(2),
            attempts: 1,
            reason: reason.to_owned(),
        })
        .unwrap()
    }

    #[test]
    fn a_clean_capture_audits_healthy() {
        let output = temp_directory("clean");
        write_records(
            &output,
            vec![
                (depth_frame(100, 110), CaptureFlags::NONE),
                (depth_frame(111, 120), CaptureFlags::NONE),
                (depth_frame(121, 130), CaptureFlags::NONE),
            ],
            2,
        );

        let report = check(&output).unwrap();

        assert_eq!(report.chunks, 2);
        assert_eq!(report.records, 3);
        assert_eq!(report.venue_frames, 3);
        assert_eq!(report.checked_frames, 3);
        assert_eq!(report.unchecked_frames, 0);
        assert_eq!(report.manifest_frames, Some(3));
        assert!(report.is_healthy());

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_tampered_chunk_fails_the_audit() {
        let output = temp_directory("tamper");
        write_records(
            &output,
            vec![(depth_frame(100, 110), CaptureFlags::NONE)],
            10,
        );

        let index = store::read_index(&output.join(FRAMES_DIR)).unwrap();
        let path = output.join(FRAMES_DIR).join(&index[0].name);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            check(&output),
            Err(RecordError::Store(store::StoreError::Integrity(_)))
        ));

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_sequence_break_is_reported_not_hidden() {
        let output = temp_directory("seq-break");
        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();

        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), 10).unwrap();
        for seq in [0u64, 1, 5] {
            writer
                .append(&CaptureRecord {
                    seq,
                    instrument: instrument(),
                    channel: Channel::BookDiff,
                    ts_socket: Timestamp::from_unix_nanos(seq as i64),
                    ts_exchange: None,
                    payload: depth_frame(100 + seq * 10, 109 + seq * 10),
                    flags: CaptureFlags::NONE,
                })
                .unwrap();
        }
        writer.finish().unwrap();

        let manifest = CaptureManifest {
            schema_version: SCHEMA_VERSION,
            capture_id: CaptureId::new("check-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument(),
            channel: Channel::BookDiff,
            frames_written: 3,
            stop_reason: None,
        };
        crate::capture::write_manifest(&output, &manifest).unwrap();

        let report = check(&output).unwrap();

        assert_eq!(
            report.seq_breaks,
            vec![SeqBreak {
                expected: 2,
                found: 5
            }]
        );
        assert!(!report.is_healthy());

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn an_update_id_gap_is_detected_offline() {
        let output = temp_directory("update-gap");
        write_records(
            &output,
            vec![
                (depth_frame(100, 110), CaptureFlags::NONE),
                (depth_frame(200, 210), CaptureFlags::NONE),
            ],
            10,
        );

        let report = check(&output).unwrap();

        assert_eq!(
            report.update_id_gaps,
            vec![UpdateIdGap {
                expected: 111,
                found: 200
            }]
        );
        assert!(!report.is_healthy());

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn gap_records_are_listed_with_reasons() {
        let output = temp_directory("gaps");
        write_records(
            &output,
            vec![
                (depth_frame(100, 110), CaptureFlags::NONE),
                (
                    gap_payload("venue_close"),
                    CaptureFlags::SYNTHETIC
                        .union(CaptureFlags::SEQUENCE_GAP)
                        .union(CaptureFlags::UNRELIABLE),
                ),
                (depth_frame(111, 120), CaptureFlags::NONE),
            ],
            10,
        );

        let report = check(&output).unwrap();

        assert_eq!(report.synthetic_records, 1);
        assert_eq!(report.connection_gaps, 1);
        assert_eq!(report.gap_details.len(), 1);
        assert_eq!(report.gap_details[0].reason, "venue_close");
        assert_eq!(report.seq_breaks, vec![]);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn unparseable_frames_count_as_unchecked() {
        let output = temp_directory("unchecked");
        write_records(
            &output,
            vec![
                (depth_frame(100, 110), CaptureFlags::NONE),
                (b"not json".to_vec(), CaptureFlags::NONE),
            ],
            10,
        );

        let report = check(&output).unwrap();

        assert_eq!(report.checked_frames, 1);
        assert_eq!(report.unchecked_frames, 1);
        assert!(report.is_healthy());

        std::fs::remove_dir_all(&output).unwrap();
    }
}
