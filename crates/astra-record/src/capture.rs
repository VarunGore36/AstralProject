use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use astra_types::{
    CaptureFlags, CaptureId, CaptureManifest, CaptureRecord, Channel, GapMarker, Instrument,
    SCHEMA_VERSION, Timestamp,
};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, connect};

use astra_book::UpdateSpan;

use crate::error::RecordError;
use crate::feed;
use crate::store::ChunkWriter;

pub const MANIFEST_FILE: &str = "manifest.json";
pub const FRAMES_DIR: &str = "frames";
pub const RECORDS_PER_CHUNK: usize = 2_000;
pub const READ_POLL: Duration = Duration::from_millis(250);
pub const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
pub const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);

pub const STOP_IN_PROGRESS: &str = "in_progress";
pub const STOP_INTERRUPTED: &str = "interrupted";
pub const STOP_MAX_FRAMES: &str = "max_frames_reached";
pub const STOP_DURATION: &str = "duration_elapsed";
pub const STOP_VENUE_CLOSED: &str = "connection_closed_by_venue";
pub const STOP_RECONNECT_EXHAUSTED: &str = "reconnect_exhausted";

pub const GAP_VENUE_CLOSE: &str = "venue_close";
pub const GAP_READ_ERROR: &str = "read_error";

#[derive(Clone, Debug)]
pub struct CaptureOptions {
    pub output: PathBuf,
    pub instrument: Instrument,
    pub channel: Channel,
    pub url: String,
    pub max_frames: Option<u64>,
    pub duration: Option<Duration>,
    pub max_reconnects: u32,
}

#[derive(Clone, Debug)]
pub struct CaptureOutcome {
    pub capture_id: String,
    pub frames_written: u64,
    pub checked_frames: u64,
    pub connection_gaps: u64,
    pub sequence_gaps: u64,
    pub stop_reason: String,
}

#[derive(Default)]
struct CaptureState {
    seq: u64,
    frames: u64,
    connection_gaps: u64,
    last_frame_at: Option<Timestamp>,
}

#[derive(Default)]
struct SequenceTracker {
    previous_last: Option<u64>,
    checked: u64,
    gaps: u64,
}

impl SequenceTracker {
    fn reset(&mut self) {
        self.previous_last = None;
    }

    fn check(&mut self, span: Option<UpdateSpan>) -> Option<String> {
        let span = span?;
        self.checked += 1;

        let previous = self.previous_last.replace(span.last)?;

        let expected = previous + 1;
        if span.first == expected {
            return None;
        }

        self.gaps += 1;
        Some(format!(
            "update_id_gap: expected {expected}, saw {}",
            span.first
        ))
    }
}

enum Disconnect {
    VenueClose,
    ReadError(String),
}

impl Disconnect {
    fn gap_reason(&self) -> String {
        match self {
            Disconnect::VenueClose => GAP_VENUE_CLOSE.to_owned(),
            Disconnect::ReadError(error) => format!("{GAP_READ_ERROR}: {error}"),
        }
    }

    fn stop_reason(&self) -> String {
        match self {
            Disconnect::VenueClose => STOP_VENUE_CLOSED.to_owned(),
            Disconnect::ReadError(error) => format!("{GAP_READ_ERROR}: {error}"),
        }
    }
}

pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub fn init_capture(
    output: &Path,
    instrument: &Instrument,
    channel: Channel,
) -> Result<CaptureManifest, RecordError> {
    std::fs::create_dir_all(output.join(FRAMES_DIR))?;

    let manifest = CaptureManifest {
        schema_version: SCHEMA_VERSION,
        capture_id: CaptureId::new(uuid::Uuid::new_v4().to_string()),
        created_at: Timestamp::now(),
        instrument: instrument.clone(),
        channel,
        frames_written: 0,
        stop_reason: Some(STOP_IN_PROGRESS.to_owned()),
    };

    write_manifest(output, &manifest)?;

    Ok(manifest)
}

pub fn write_manifest(output: &Path, manifest: &CaptureManifest) -> Result<(), RecordError> {
    std::fs::write(
        output.join(MANIFEST_FILE),
        serde_json::to_string_pretty(manifest)?,
    )?;
    Ok(())
}

pub fn run_capture(
    options: CaptureOptions,
    interrupted: Arc<AtomicBool>,
) -> Result<CaptureOutcome, RecordError> {
    install_crypto_provider();

    let mut manifest = init_capture(&options.output, &options.instrument, options.channel)?;
    let mut writer = ChunkWriter::open(options.output.join(FRAMES_DIR), RECORDS_PER_CHUNK)?;

    let mut socket = open_connection(&options.url)?;
    let mut state = CaptureState::default();
    let mut tracker = SequenceTracker::default();
    let mut session_started = Timestamp::now();
    let mut reconnects_used = 0u32;
    let mut backoff = RECONNECT_INITIAL_BACKOFF;
    let started = Instant::now();

    let stop_reason = loop {
        if interrupted.load(Ordering::SeqCst) {
            break STOP_INTERRUPTED.to_owned();
        }
        if options
            .max_frames
            .is_some_and(|limit| state.frames >= limit)
        {
            break STOP_MAX_FRAMES.to_owned();
        }
        if options
            .duration
            .is_some_and(|limit| started.elapsed() >= limit)
        {
            break STOP_DURATION.to_owned();
        }

        let disconnect = match socket.read() {
            Ok(Message::Text(text)) => {
                append_frame(
                    &mut writer,
                    &options,
                    &mut state,
                    &mut tracker,
                    text.as_bytes(),
                )?;
                continue;
            }
            Ok(Message::Binary(bytes)) => {
                append_frame(&mut writer, &options, &mut state, &mut tracker, &bytes)?;
                continue;
            }
            Ok(Message::Close(_)) => Disconnect::VenueClose,
            Ok(_) => continue,
            Err(error) if is_poll_timeout(&error) => continue,
            Err(error) => Disconnect::ReadError(error.to_string()),
        };

        if reconnects_used >= options.max_reconnects {
            break if options.max_reconnects == 0 {
                disconnect.stop_reason()
            } else {
                STOP_RECONNECT_EXHAUSTED.to_owned()
            };
        }

        reconnects_used += 1;
        let gap_started = state.last_frame_at.unwrap_or(session_started);
        thread::sleep(backoff);
        backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);

        if interrupted.load(Ordering::SeqCst) {
            break STOP_INTERRUPTED.to_owned();
        }

        socket = match open_connection(&options.url) {
            Ok(socket) => socket,
            Err(error) => break format!("reconnect_failed: {error}"),
        };
        session_started = Timestamp::now();

        append_gap(
            &mut writer,
            &options,
            &mut state,
            gap_started,
            session_started,
            reconnects_used,
            disconnect.gap_reason(),
        )?;
        state.connection_gaps += 1;
        tracker.reset();
    };

    writer.finish()?;

    manifest.frames_written = state.frames;
    manifest.stop_reason = Some(stop_reason.clone());
    write_manifest(&options.output, &manifest)?;

    Ok(CaptureOutcome {
        capture_id: manifest.capture_id.as_str().to_owned(),
        frames_written: state.frames,
        checked_frames: tracker.checked,
        connection_gaps: state.connection_gaps,
        sequence_gaps: tracker.gaps,
        stop_reason,
    })
}

fn open_connection(url: &str) -> Result<WebSocket<MaybeTlsStream<TcpStream>>, RecordError> {
    let (mut socket, _response) = connect(url)?;
    set_read_timeout(&mut socket, READ_POLL)?;
    Ok(socket)
}

fn append_frame(
    writer: &mut ChunkWriter,
    options: &CaptureOptions,
    state: &mut CaptureState,
    tracker: &mut SequenceTracker,
    payload: &[u8],
) -> Result<(), RecordError> {
    let span = feed::update_span(options.instrument.venue(), options.channel, payload);

    if let Some(reason) = tracker.check(span) {
        let now = Timestamp::now();
        let started_at = state.last_frame_at.unwrap_or(now);
        append_gap(writer, options, state, started_at, now, 0, reason)?;
    }

    let record = CaptureRecord {
        seq: state.seq,
        instrument: options.instrument.clone(),
        channel: options.channel,
        ts_socket: Timestamp::now(),
        ts_exchange: None,
        payload: payload.to_vec(),
        flags: CaptureFlags::NONE,
    };

    writer.append(&record)?;
    state.seq += 1;
    state.frames += 1;
    state.last_frame_at = Some(record.ts_socket);

    Ok(())
}

fn append_gap(
    writer: &mut ChunkWriter,
    options: &CaptureOptions,
    state: &mut CaptureState,
    started_at: Timestamp,
    ended_at: Timestamp,
    attempts: u32,
    reason: String,
) -> Result<(), RecordError> {
    let marker = GapMarker {
        started_at,
        ended_at,
        attempts,
        reason,
    };

    let record = CaptureRecord {
        seq: state.seq,
        instrument: options.instrument.clone(),
        channel: options.channel,
        ts_socket: ended_at,
        ts_exchange: None,
        payload: serde_json::to_vec(&marker)?,
        flags: CaptureFlags::SYNTHETIC
            .union(CaptureFlags::SEQUENCE_GAP)
            .union(CaptureFlags::UNRELIABLE),
    };

    writer.append(&record)?;
    state.seq += 1;

    Ok(())
}

fn is_poll_timeout(error: &tungstenite::Error) -> bool {
    matches!(
        error,
        tungstenite::Error::Io(inner)
            if matches!(
                inner.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            )
    )
}

fn set_read_timeout(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    timeout: Duration,
) -> Result<(), RecordError> {
    let tcp = match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => stream,
        MaybeTlsStream::Rustls(stream) => &mut stream.sock,
        _ => return Err(RecordError::TransportUnsupported),
    };

    tcp.set_read_timeout(Some(timeout))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store;
    use astra_types::{MarketType, Symbol, Venue};
    use std::net::TcpListener;

    fn instrument() -> Instrument {
        Instrument::new(
            Venue::Binance,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    fn temp_directory(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("astra-capture-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn serve_connections(connections: Vec<Vec<String>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        thread::spawn(move || {
            for frames in connections {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                let Ok(mut socket) = tungstenite::accept(stream) else {
                    return;
                };
                for frame in frames {
                    let _ = socket.send(Message::text(frame));
                }
                thread::sleep(Duration::from_millis(20));
                let _ = socket.close(None);
            }
        });

        format!("ws://{address}")
    }

    fn serve_open() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let _socket = tungstenite::accept(stream);
                thread::sleep(Duration::from_secs(5));
            }
        });

        format!("ws://{address}")
    }

    fn options(output: &Path, url: String) -> CaptureOptions {
        CaptureOptions {
            output: output.to_owned(),
            instrument: instrument(),
            channel: Channel::BookDiff,
            url,
            max_frames: None,
            duration: None,
            max_reconnects: 0,
        }
    }

    fn read_manifest(output: &Path) -> CaptureManifest {
        let body = std::fs::read_to_string(output.join(MANIFEST_FILE)).unwrap();
        serde_json::from_str(&body).unwrap()
    }

    fn read_gaps(output: &Path) -> Vec<GapMarker> {
        store::read_all(&output.join(FRAMES_DIR))
            .unwrap()
            .into_iter()
            .filter(|record| record.flags.contains(CaptureFlags::SYNTHETIC))
            .map(|record| serde_json::from_slice(&record.payload).unwrap())
            .collect()
    }

    fn depth_frame(first: u64, last: u64) -> String {
        format!(r#"{{"e":"depthUpdate","s":"BTCUSDT","U":{first},"u":{last},"b":[],"a":[]}}"#)
    }

    #[test]
    fn init_writes_layout_and_manifest() {
        let output = temp_directory("init");
        let manifest = init_capture(&output, &instrument(), Channel::BookDiff).unwrap();

        assert!(output.join(FRAMES_DIR).is_dir());
        assert_eq!(read_manifest(&output), manifest);
        assert_eq!(manifest.schema_version, SCHEMA_VERSION);
        assert_eq!(manifest.frames_written, 0);
        assert_eq!(manifest.stop_reason.as_deref(), Some(STOP_IN_PROGRESS));

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn frames_are_recorded_with_exact_payloads() {
        let url = serve_connections(vec![vec![
            "{\"a\":1}".to_owned(),
            "héllo".to_owned(),
            String::new(),
        ]]);
        let output = temp_directory("payloads");

        let outcome = run_capture(options(&output, url), Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 3);
        assert_eq!(outcome.connection_gaps, 0);
        assert_eq!(outcome.sequence_gaps, 0);
        assert_eq!(outcome.checked_frames, 0);
        assert_eq!(outcome.stop_reason, STOP_VENUE_CLOSED);

        let records = store::read_all(&output.join(FRAMES_DIR)).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].payload, b"{\"a\":1}");
        assert_eq!(records[1].payload, "héllo".as_bytes());
        assert_eq!(records[2].payload, Vec::<u8>::new());
        assert_eq!(records[0].seq, 0);
        assert_eq!(records[2].seq, 2);
        assert!(records[0].ts_socket.unix_nanos() > 0);
        assert_eq!(records[0].instrument, instrument());

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn reconnect_records_a_gap_and_keeps_capturing() {
        let url = serve_connections(vec![
            vec!["first-0".to_owned(), "first-1".to_owned()],
            vec![
                "second-0".to_owned(),
                "second-1".to_owned(),
                "second-2".to_owned(),
            ],
        ]);
        let output = temp_directory("reconnect");
        let mut options = options(&output, url);
        options.max_reconnects = 1;

        let outcome = run_capture(options, Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 5);
        assert_eq!(outcome.connection_gaps, 1);
        assert_eq!(outcome.sequence_gaps, 0);
        assert_eq!(outcome.stop_reason, STOP_RECONNECT_EXHAUSTED);

        let records = store::read_all(&output.join(FRAMES_DIR)).unwrap();
        assert_eq!(records.len(), 6);
        assert_eq!(records[2].seq, 2);
        assert!(records[2].flags.contains(CaptureFlags::SYNTHETIC));
        assert!(records[2].flags.contains(CaptureFlags::SEQUENCE_GAP));
        assert!(records[2].flags.contains(CaptureFlags::UNRELIABLE));
        assert!(!records[2].payload.is_empty());
        assert_eq!(records[5].seq, 5);

        let gaps = read_gaps(&output);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].attempts, 1);
        assert_eq!(gaps[0].reason, GAP_VENUE_CLOSE);
        assert!(gaps[0].ended_at > gaps[0].started_at);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn gap_span_matches_the_last_frame_before_the_drop() {
        let url = serve_connections(vec![vec!["only".to_owned()], vec!["after".to_owned()]]);
        let output = temp_directory("gap-span");
        let mut options = options(&output, url);
        options.max_reconnects = 1;

        run_capture(options, Arc::new(AtomicBool::new(false))).unwrap();

        let records = store::read_all(&output.join(FRAMES_DIR)).unwrap();
        let gaps = read_gaps(&output);
        assert_eq!(records[0].ts_socket, gaps[0].started_at);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn continuous_update_ids_produce_no_gaps() {
        let url = serve_connections(vec![vec![
            depth_frame(100, 110),
            depth_frame(111, 120),
            depth_frame(121, 130),
        ]]);
        let output = temp_directory("continuous");
        let options = options(&output, url);

        let outcome = run_capture(options, Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 3);
        assert_eq!(outcome.checked_frames, 3);
        assert_eq!(outcome.sequence_gaps, 0);
        assert_eq!(outcome.connection_gaps, 0);
        assert_eq!(store::read_all(&output.join(FRAMES_DIR)).unwrap().len(), 3);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn update_id_gaps_are_detected() {
        let url = serve_connections(vec![vec![
            depth_frame(100, 110),
            depth_frame(111, 120),
            depth_frame(200, 210),
        ]]);
        let output = temp_directory("sequence-gap");
        let options = options(&output, url);

        let outcome = run_capture(options, Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 3);
        assert_eq!(outcome.checked_frames, 3);
        assert_eq!(outcome.sequence_gaps, 1);
        assert_eq!(outcome.connection_gaps, 0);

        let records = store::read_all(&output.join(FRAMES_DIR)).unwrap();
        assert_eq!(records.len(), 4);
        assert!(records[2].flags.contains(CaptureFlags::SYNTHETIC));
        assert!(records[2].flags.contains(CaptureFlags::SEQUENCE_GAP));
        assert!(records[2].flags.contains(CaptureFlags::UNRELIABLE));
        assert_eq!(records[2].seq, 2);
        assert_eq!(records[3].seq, 3);

        let gaps = read_gaps(&output);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].attempts, 0);
        assert!(gaps[0].reason.contains("update_id_gap"));
        assert!(gaps[0].reason.contains("expected 121"));
        assert!(gaps[0].reason.contains("saw 200"));

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn the_sequence_gap_precedes_the_frame_that_revealed_it() {
        let first = depth_frame(100, 110);
        let second = depth_frame(500, 510);
        let url = serve_connections(vec![vec![first.clone(), second.clone()]]);
        let output = temp_directory("gap-order");
        let options = options(&output, url);

        run_capture(options, Arc::new(AtomicBool::new(false))).unwrap();

        let records = store::read_all(&output.join(FRAMES_DIR)).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].payload, first.as_bytes());
        assert!(records[1].flags.contains(CaptureFlags::SEQUENCE_GAP));
        assert_eq!(records[2].payload, second.as_bytes());

        let gaps = read_gaps(&output);
        assert_eq!(gaps[0].started_at, records[0].ts_socket);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn max_frames_stops_the_capture_cleanly() {
        let url = serve_connections(vec![(0..10).map(|i| format!("frame-{i}")).collect()]);
        let output = temp_directory("max-frames");
        let mut options = options(&output, url);
        options.max_frames = Some(3);

        let outcome = run_capture(options, Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 3);
        assert_eq!(outcome.stop_reason, STOP_MAX_FRAMES);
        assert_eq!(store::read_all(&output.join(FRAMES_DIR)).unwrap().len(), 3);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn manifest_records_the_stop_reason() {
        let url = serve_connections(vec![vec!["{}".to_owned()]]);
        let output = temp_directory("manifest");

        run_capture(options(&output, url), Arc::new(AtomicBool::new(false))).unwrap();

        let manifest = read_manifest(&output);
        assert_eq!(manifest.frames_written, 1);
        assert_eq!(manifest.stop_reason.as_deref(), Some(STOP_VENUE_CLOSED));

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn interruption_stops_before_capturing() {
        let url = serve_connections(vec![vec!["{}".to_owned()]]);
        let output = temp_directory("interrupted");
        let interrupted = Arc::new(AtomicBool::new(false));
        interrupted.store(true, Ordering::SeqCst);

        let outcome = run_capture(options(&output, url), interrupted).unwrap();

        assert_eq!(outcome.frames_written, 0);
        assert_eq!(outcome.stop_reason, STOP_INTERRUPTED);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn quiet_feeds_still_respect_the_duration_limit() {
        let url = serve_open();
        let output = temp_directory("duration");
        let mut options = options(&output, url);
        options.duration = Some(Duration::from_millis(100));

        let outcome = run_capture(options, Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 0);
        assert_eq!(outcome.stop_reason, STOP_DURATION);

        std::fs::remove_dir_all(&output).unwrap();
    }
}
