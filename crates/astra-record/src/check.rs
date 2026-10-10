use std::path::Path;

use astra_types::{CaptureFlags, CaptureManifest, GapMarker, SCHEMA_VERSION, Timestamp};

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
    /// (claimed, actual venue frames) when the manifest count disagrees with
    /// the chunks on disk. Replay and normalize already refuse such captures;
    /// the audit reports it instead of aborting, so the finding is visible.
    pub manifest_mismatch: Option<(u64, u64)>,
    pub stop_reason: Option<String>,
}

impl CheckReport {
    pub fn is_healthy(&self) -> bool {
        self.seq_breaks.is_empty()
            && self.update_id_gaps.is_empty()
            && self.undecodable_gaps == 0
            && self.manifest_mismatch.is_none()
    }
}

pub fn check(input: &Path) -> Result<CheckReport, RecordError> {
    let manifest = read_manifest(input)?;
    let frames_dir = input.join(FRAMES_DIR);
    let index = store::read_index(&frames_dir)?;

    let mut report = CheckReport {
        manifest_frames: Some(manifest.frames_written),
        stop_reason: manifest.stop_reason.clone(),
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
            }
            // Wrapping, not checked: at u64::MAX the next expected value is
            // unrepresentable, and any following record mismatches anyway. A
            // hostile chunk with seq MAX must not panic the audit.
            expected_seq = record.seq.wrapping_add(1);

            if record.flags.contains(CaptureFlags::SYNTHETIC) {
                report.synthetic_records += 1;
                match serde_json::from_slice::<GapMarker>(&record.payload) {
                    Ok(marker) => {
                        if marker.reason.starts_with("update_id_gap") {
                            // The writer records the numbers in prose
                            // ("update_id_gap: expected 121, saw 200"); the
                            // audit recovers them so its own update_gap lines
                            // carry data. Unparseable shapes keep (0, 0) and
                            // stay visible in gap_details regardless.
                            let (expected, found) =
                                parse_update_id_gap(&marker.reason).unwrap_or((0, 0));
                            report.update_id_gaps.push(UpdateIdGap { expected, found });
                        } else {
                            // A reconnect starts a fresh venue baseline (the
                            // recorder resets its tracker the same way), so
                            // the next spans are continuity-checked against
                            // the new stream, not accused of jumping from
                            // the old one.
                            previous_last = None;
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
                        // checked_add: at u64::MAX no forward jump is
                        // representable, so no gap can exist past saturation
                        // (and no panic either).
                        if previous
                            .checked_add(1)
                            .is_some_and(|expected| span.first > expected)
                        {
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

    if report.venue_frames != manifest.frames_written {
        report.manifest_mismatch = Some((manifest.frames_written, report.venue_frames));
    }

    Ok(report)
}

fn observe_timestamp(report: &mut CheckReport, timestamp: Timestamp) {
    if report.first_ts.is_none() {
        report.first_ts = Some(timestamp);
    }
    report.last_ts = Some(timestamp);
}

fn parse_update_id_gap(reason: &str) -> Option<(u64, u64)> {
    let rest = reason.strip_prefix("update_id_gap: expected ")?;
    let (expected, rest) = rest.split_once(", saw ")?;
    Some((expected.trim().parse().ok()?, rest.trim().parse().ok()?))
}

pub(crate) fn read_manifest(input: &Path) -> Result<CaptureManifest, RecordError> {
    let body = std::fs::read_to_string(input.join(MANIFEST_FILE))?;
    let manifest: CaptureManifest = serde_json::from_str(&body)?;
    // A format change must fail loudly here, not misread silently.
    if manifest.schema_version != SCHEMA_VERSION {
        return Err(RecordError::SchemaVersion {
            found: manifest.schema_version,
            expected: SCHEMA_VERSION,
        });
    }
    Ok(manifest)
}

/// Render the audit report exactly as the CLI prints it.
///
/// The format is load-bearing: `ops/soak.sh check` parses the `verdict`
/// line to judge the soak. Any change here must update the golden tests
/// below and the soak script together — never one without the others.
pub fn format_report(input: &Path, report: &CheckReport) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "input       {}", input.display());
    let _ = writeln!(out, "chunks      {}", report.chunks);
    let _ = writeln!(out, "records     {}", report.records);
    let _ = writeln!(out, "venue       {}", report.venue_frames);
    let _ = writeln!(out, "synthetic   {}", report.synthetic_records);
    let _ = writeln!(out, "checked     {}", report.checked_frames);
    let _ = writeln!(out, "unchecked   {}", report.unchecked_frames);
    let _ = writeln!(out, "conn_gaps   {}", report.connection_gaps);
    let _ = writeln!(out, "seq_gaps    {}", report.update_id_gaps.len());
    let _ = writeln!(out, "seq_breaks  {}", report.seq_breaks.len());
    let _ = writeln!(
        out,
        "manifest    {}",
        report
            .manifest_frames
            .map(|frames| frames.to_string())
            .unwrap_or_else(|| "missing".to_owned())
    );
    let _ = writeln!(
        out,
        "stop_reason {}",
        report.stop_reason.as_deref().unwrap_or("missing")
    );
    if let Some((claimed, actual)) = report.manifest_mismatch {
        let _ = writeln!(
            out,
            "manifest_mismatch claimed {claimed} venue frames but the chunks hold {actual}"
        );
    }
    let _ = writeln!(
        out,
        "span        {}",
        match (report.first_ts, report.last_ts) {
            (Some(first), Some(last)) => format!(
                "{}s first to last",
                last.unix_nanos().saturating_sub(first.unix_nanos()) as f64 / 1_000_000_000.0
            ),
            _ => "empty".to_owned(),
        }
    );

    for gap in &report.gap_details {
        let _ = writeln!(
            out,
            "gap         seq {} attempts {} {}",
            gap.seq, gap.attempts, gap.reason
        );
    }
    for id_gap in &report.update_id_gaps {
        if id_gap.expected != 0 {
            let _ = writeln!(
                out,
                "update_gap  expected {} saw {}",
                id_gap.expected, id_gap.found
            );
        }
    }
    for seq_break in &report.seq_breaks {
        let _ = writeln!(
            out,
            "seq_break   expected {} found {}",
            seq_break.expected, seq_break.found
        );
    }

    let _ = writeln!(
        out,
        "verdict     {}",
        if report.is_healthy() {
            "healthy"
        } else {
            "issues found, see above"
        }
    );

    out
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
            // Venue frames only, matching production: gap markers ride along
            // in seq order but are not venue data.
            frames_written: payloads
                .iter()
                .filter(|(_, flags)| !flags.contains(CaptureFlags::SYNTHETIC))
                .count() as u64,
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
    fn saturated_record_sequences_neither_panic_nor_hide_breaks() {
        // Same overflow class as the update-ID guards: a hostile chunk
        // carrying seq u64::MAX must audit (and report its break), not panic.
        let output = temp_directory("seq-saturated");
        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();

        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), 10).unwrap();
        for seq in [0u64, u64::MAX] {
            writer
                .append(&CaptureRecord {
                    seq,
                    instrument: instrument(),
                    channel: Channel::BookDiff,
                    ts_socket: Timestamp::from_unix_nanos(0),
                    ts_exchange: None,
                    payload: depth_frame(100, 110),
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
            frames_written: 2,
            stop_reason: None,
        };
        crate::capture::write_manifest(&output, &manifest).unwrap();

        let report = check(&output).unwrap();

        assert_eq!(
            report.seq_breaks,
            vec![SeqBreak {
                expected: 1,
                found: u64::MAX
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
    fn update_id_gap_markers_keep_their_numbers() {
        assert_eq!(
            parse_update_id_gap("update_id_gap: expected 121, saw 200"),
            Some((121, 200))
        );
        // Future shapes degrade to zeros but never panic; the reason string
        // itself always survives in gap_details.
        assert_eq!(parse_update_id_gap("update_id_gap: something new"), None);
        assert_eq!(parse_update_id_gap("venue_close"), None);
        assert_eq!(parse_update_id_gap(""), None);
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
    fn saturated_update_ids_neither_panic_nor_emit_gaps() {
        // Hostile boundary: no forward jump is representable past u64::MAX,
        // so the audit must stay silent instead of panicking.
        let output = temp_directory("saturated");
        write_records(
            &output,
            vec![
                (depth_frame(u64::MAX - 1, u64::MAX), CaptureFlags::NONE),
                (depth_frame(0, 10), CaptureFlags::NONE),
            ],
            10,
        );

        let report = check(&output).unwrap();

        assert_eq!(report.checked_frames, 2);
        assert!(report.update_id_gaps.is_empty());
        assert!(report.is_healthy());

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_doctored_manifest_is_reported_not_trusted() {
        // Replay and normalize already refuse captures whose manifest count
        // disagrees with the chunks; the audit must now say so too instead of
        // passing them silently.
        let output = temp_directory("doctored");
        write_records(
            &output,
            vec![
                (depth_frame(100, 110), CaptureFlags::NONE),
                (depth_frame(111, 120), CaptureFlags::NONE),
            ],
            10,
        );

        let manifest_path = output.join(crate::capture::MANIFEST_FILE);
        let body = std::fs::read_to_string(&manifest_path).unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(&body).unwrap();
        manifest["frames_written"] = serde_json::json!(99);
        manifest["stop_reason"] = serde_json::json!("duration_elapsed");
        std::fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        let report = check(&output).unwrap();

        assert_eq!(report.manifest_mismatch, Some((99, 2)));
        assert_eq!(report.stop_reason.as_deref(), Some("duration_elapsed"));
        assert!(!report.is_healthy());

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_future_schema_version_is_refused_not_misread() {
        let output = temp_directory("schema-version");
        write_records(
            &output,
            vec![(depth_frame(100, 110), CaptureFlags::NONE)],
            10,
        );

        let manifest_path = output.join(crate::capture::MANIFEST_FILE);
        let body = std::fs::read_to_string(&manifest_path).unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(&body).unwrap();
        manifest["schema_version"] = serde_json::json!(SCHEMA_VERSION + 1);
        std::fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        assert!(matches!(
            check(&output),
            Err(crate::error::RecordError::SchemaVersion { .. })
        ));

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn the_report_format_is_pinned_for_the_soak_judge() {
        // ops/soak.sh check parses the verdict line. If this format moves,
        // the judge breaks silently — so the format is asserted line by line.
        let healthy = CheckReport {
            chunks: 2,
            records: 601,
            venue_frames: 601,
            checked_frames: 601,
            manifest_frames: Some(601),
            stop_reason: Some("duration_elapsed".to_owned()),
            first_ts: Some(Timestamp::from_unix_nanos(1)),
            last_ts: Some(Timestamp::from_unix_nanos(2_000_000_000)),
            ..CheckReport::default()
        };
        let text = format_report(std::path::Path::new("./capture"), &healthy);
        for line in [
            "input       ./capture",
            "chunks      2",
            "records     601",
            "venue       601",
            "synthetic   0",
            "checked     601",
            "unchecked   0",
            "conn_gaps   0",
            "seq_gaps    0",
            "seq_breaks  0",
            "manifest    601",
            "stop_reason duration_elapsed",
            "verdict     healthy",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }
        assert!(
            !text.contains("manifest_mismatch"),
            "clean report leaked: {text}"
        );

        let broken = CheckReport {
            venue_frames: 2,
            checked_frames: 2,
            manifest_frames: Some(99),
            manifest_mismatch: Some((99, 2)),
            stop_reason: Some("duration_elapsed".to_owned()),
            update_id_gaps: vec![UpdateIdGap {
                expected: 121,
                found: 200,
            }],
            seq_breaks: vec![SeqBreak {
                expected: 2,
                found: 5,
            }],
            ..CheckReport::default()
        };
        let text = format_report(std::path::Path::new("./capture"), &broken);
        for line in [
            "manifest_mismatch claimed 99 venue frames but the chunks hold 2",
            "update_gap  expected 121 saw 200",
            "seq_break   expected 2 found 5",
            "verdict     issues found, see above",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }
    }
    #[test]
    fn captured_gap_numbers_reach_the_report() {
        let output = temp_directory("gap-numbers");
        write_records(
            &output,
            vec![
                (depth_frame(100, 110), CaptureFlags::NONE),
                (
                    gap_payload("update_id_gap: expected 121, saw 200"),
                    CaptureFlags::SYNTHETIC
                        .union(CaptureFlags::SEQUENCE_GAP)
                        .union(CaptureFlags::UNRELIABLE),
                ),
                (depth_frame(200, 210), CaptureFlags::NONE),
            ],
            10,
        );

        let report = check(&output).unwrap();

        // The marker's own numbers plus the independently detected span jump
        // (200 > 111): belt and suspenders, both true, both reported.
        assert_eq!(
            report.update_id_gaps,
            vec![
                UpdateIdGap {
                    expected: 121,
                    found: 200
                },
                UpdateIdGap {
                    expected: 111,
                    found: 200
                },
            ]
        );
        let text = format_report(std::path::Path::new("./capture"), &report);
        assert!(text.contains("update_gap  expected 121 saw 200"), "{text}");

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn reconnects_reset_the_baseline_instead_of_crying_wolf() {
        // A fresh venue baseline after a reconnect is not a sequence jump:
        // the audit resets like the recorder does, so a clean reconnect
        // reports connection_gaps 1 with no update_id_gap finding.
        let output = temp_directory("reconnect-baseline");
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
                (depth_frame(500, 510), CaptureFlags::NONE),
            ],
            10,
        );

        let report = check(&output).unwrap();

        assert_eq!(report.connection_gaps, 1);
        assert!(report.update_id_gaps.is_empty());
        assert!(report.is_healthy());

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
