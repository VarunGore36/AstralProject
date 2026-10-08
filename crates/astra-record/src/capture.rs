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

use astra_book::{Reconstructor, UpdateSpan};

use crate::error::RecordError;
use crate::feed;
use crate::store::ChunkWriter;

pub const MANIFEST_FILE: &str = "manifest.json";
pub const FRAMES_DIR: &str = "frames";
pub const RECORDS_PER_CHUNK: usize = 2_000;
pub const READ_POLL: Duration = Duration::from_millis(250);
pub const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(250);

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
    pub fanouts: Vec<Fanout>,
}

#[derive(Clone, Debug)]
pub struct Fanout {
    pub dir: PathBuf,
    pub channel: Channel,
}

#[derive(Clone, Debug)]
pub struct CaptureOutcome {
    pub capture_id: String,
    pub frames_written: u64,
    pub checked_frames: u64,
    pub connection_gaps: u64,
    pub sequence_gaps: u64,
    pub stop_reason: String,
    pub latency: LatencySummary,
    pub book_latency: LatencySummary,
    pub book_updates: u64,
    /// Live-book applies that failed (bad levels, broken book). Counted, not
    /// hidden: a capture whose book silently stopped tracking must not look
    /// identical to a healthy one.
    pub book_errors: u64,
    pub fanouts: Vec<FanoutOutcome>,
    pub unknown_frames: u64,
    pub first_unknown_stream: Option<String>,
}

#[derive(Clone, Debug)]
pub struct FanoutOutcome {
    pub capture_id: String,
    pub channel: Channel,
    pub dir: PathBuf,
    pub frames_written: u64,
    pub checked_frames: u64,
    pub connection_gaps: u64,
    pub sequence_gaps: u64,
}

#[derive(Default)]
struct CaptureState {
    seq: u64,
    frames: u64,
    connection_gaps: u64,
    last_frame_at: Option<Timestamp>,
}

#[derive(Clone, Debug, Default)]
pub struct LatencySummary {
    pub samples: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

#[derive(Default)]
struct LatencyTracker {
    samples_ns: Vec<u64>,
}

impl LatencyTracker {
    fn record(&mut self, elapsed: Duration) {
        self.samples_ns
            .push(elapsed.as_nanos().min(u64::MAX as u128) as u64);
    }

    fn summary(&self) -> Option<LatencySummary> {
        if self.samples_ns.is_empty() {
            return None;
        }

        let mut sorted = self.samples_ns.clone();
        sorted.sort_unstable();

        let percentile =
            |p: f64| sorted[((p * sorted.len() as f64) as usize).min(sorted.len() - 1)];

        Some(LatencySummary {
            samples: sorted.len() as u64,
            p50_ns: percentile(0.50),
            p99_ns: percentile(0.99),
            max_ns: sorted[sorted.len() - 1],
        })
    }
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

        let Some(expected) = previous.checked_add(1) else {
            // Update IDs saturated at u64::MAX: no forward jump is
            // representable past saturation, so no gap can be demonstrated
            // (and, critically, none panics on hostile input).
            return None;
        };
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

fn has_chunks(dir: &Path) -> Result<bool, RecordError> {
    let frames = dir.join(FRAMES_DIR);
    if !frames.exists() {
        return Ok(false);
    }
    for entry in std::fs::read_dir(&frames)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if name.starts_with("chunk-") && name.ends_with(".zst") {
            return Ok(true);
        }
    }
    Ok(false)
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

/// Render the init report exactly as the CLI prints it.
///
/// Golden-tested with the other report printers: init output is the first
/// thing a new user sees, so its format is pinned like the rest.
pub fn format_init_report(output: &Path, manifest: &CaptureManifest) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "capture_id  {}", manifest.capture_id.as_str());
    let _ = writeln!(out, "output      {}", output.display());
    let _ = writeln!(out, "instrument  {}", manifest.instrument);
    let _ = writeln!(out, "channel     {}", manifest.channel);
    let _ = writeln!(out, "frames      {}", manifest.frames_written);
    let _ = writeln!(out, "manifest    {}", output.join(MANIFEST_FILE).display());

    out
}

/// Render the capture outcome exactly as the CLI prints it.
///
/// The `frames` line is load-bearing: `ops/soak.sh status` greps it out of
/// capture logs. Golden-tested below; change format and test together.
pub fn format_outcome(
    output: &Path,
    url: &str,
    instrument: &Instrument,
    channel: Channel,
    outcome: &CaptureOutcome,
) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "capture_id  {}", outcome.capture_id);
    let _ = writeln!(out, "output      {}", output.display());
    let _ = writeln!(out, "url         {url}");
    let _ = writeln!(out, "instrument  {instrument}");
    let _ = writeln!(out, "channel     {channel}");
    let _ = writeln!(out, "frames      {}", outcome.frames_written);
    let _ = writeln!(out, "checked     {}", outcome.checked_frames);
    let _ = writeln!(out, "conn_gaps   {}", outcome.connection_gaps);
    let _ = writeln!(out, "seq_gaps    {}", outcome.sequence_gaps);
    for fanout in &outcome.fanouts {
        let _ = writeln!(
            out,
            "fanout      {} {} frames={} checked={} gaps={}",
            fanout.dir.display(),
            fanout.channel,
            fanout.frames_written,
            fanout.checked_frames,
            fanout.connection_gaps + fanout.sequence_gaps,
        );
    }
    if outcome.unknown_frames > 0 {
        let _ = writeln!(
            out,
            "unknown     {} (first: {})",
            outcome.unknown_frames,
            outcome
                .first_unknown_stream
                .as_deref()
                .unwrap_or("unparseable")
        );
    }
    let _ = writeln!(
        out,
        "latency_us  p50 {:.1} p99 {:.1} max {:.1} ({} frames, read to stored)",
        outcome.latency.p50_ns as f64 / 1_000.0,
        outcome.latency.p99_ns as f64 / 1_000.0,
        outcome.latency.max_ns as f64 / 1_000.0,
        outcome.latency.samples
    );
    let _ = writeln!(
        out,
        "book_us     p50 {:.1} p99 {:.1} max {:.1} (updates {}, errors {}, read to book-updated)",
        outcome.book_latency.p50_ns as f64 / 1_000.0,
        outcome.book_latency.p99_ns as f64 / 1_000.0,
        outcome.book_latency.max_ns as f64 / 1_000.0,
        outcome.book_updates,
        outcome.book_errors
    );
    let _ = writeln!(out, "stop_reason {}", outcome.stop_reason);
    let _ = writeln!(out, "manifest    {}", output.join(MANIFEST_FILE).display());

    out
}

struct Session {
    dir: PathBuf,
    channel: Channel,
    manifest: CaptureManifest,
    writer: ChunkWriter,
    state: CaptureState,
    tracker: SequenceTracker,
}

impl Session {
    fn open(dir: &Path, instrument: &Instrument, channel: Channel) -> Result<Self, RecordError> {
        // Never capture into a directory that already holds chunks: the
        // manifest would be rewritten and seq restarted at zero while old
        // chunks stay on disk, producing sequence collisions that look like
        // venue data. Resume is future work; silent corruption is not an
        // interim step. All live paths (including ops/soak.sh) use fresh
        // directories.
        if has_chunks(dir)? {
            return Err(RecordError::CaptureExists(dir.to_owned()));
        }
        let manifest = init_capture(dir, instrument, channel)?;
        let writer = ChunkWriter::open(dir.join(FRAMES_DIR), RECORDS_PER_CHUNK)?;
        Ok(Session {
            dir: dir.to_owned(),
            channel,
            manifest,
            writer,
            state: CaptureState::default(),
            tracker: SequenceTracker::default(),
        })
    }

    fn finish(mut self, stop_reason: &str) -> Result<FanoutOutcome, RecordError> {
        self.writer.finish()?;

        self.manifest.frames_written = self.state.frames;
        self.manifest.stop_reason = Some(stop_reason.to_owned());
        write_manifest(&self.dir, &self.manifest)?;

        Ok(FanoutOutcome {
            capture_id: self.manifest.capture_id.as_str().to_owned(),
            channel: self.channel,
            dir: self.dir,
            frames_written: self.state.frames,
            checked_frames: self.tracker.checked,
            connection_gaps: self.state.connection_gaps,
            sequence_gaps: self.tracker.gaps,
        })
    }

    fn append_gap(
        &mut self,
        instrument: &Instrument,
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
            seq: self.state.seq,
            instrument: instrument.clone(),
            channel: self.channel,
            ts_socket: ended_at,
            ts_exchange: None,
            payload: serde_json::to_vec(&marker)?,
            flags: CaptureFlags::SYNTHETIC
                .union(CaptureFlags::SEQUENCE_GAP)
                .union(CaptureFlags::UNRELIABLE),
        };

        self.writer.append(&record)?;
        self.state.seq += 1;

        Ok(())
    }
}

struct UnknownFrames {
    count: u64,
    first_stream: Option<String>,
}

pub fn run_capture(
    options: CaptureOptions,
    interrupted: Arc<AtomicBool>,
) -> Result<CaptureOutcome, RecordError> {
    install_crypto_provider();

    let combined = !options.fanouts.is_empty();
    if combined {
        let channels: Vec<Channel> = std::iter::once(options.channel)
            .chain(options.fanouts.iter().map(|fanout| fanout.channel))
            .collect();
        feed::combined_stream_url(&options.instrument, &channels)?;
    }

    let mut sessions = vec![Session::open(
        &options.output,
        &options.instrument,
        options.channel,
    )?];
    for fanout in &options.fanouts {
        sessions.push(Session::open(
            &fanout.dir,
            &options.instrument,
            fanout.channel,
        )?);
    }

    let mut socket = open_connection(&options)?;
    let mut latency = LatencyTracker::default();
    let mut live_book = Reconstructor::new();
    let mut book_latency = LatencyTracker::default();
    let mut book_updates = 0u64;
    let mut book_errors = 0u64;
    let mut unknown = UnknownFrames {
        count: 0,
        first_stream: None,
    };
    let mut reconnects_used = 0u32;
    let started = Instant::now();

    let stop_reason = loop {
        if interrupted.load(Ordering::SeqCst) {
            break STOP_INTERRUPTED.to_owned();
        }
        if options
            .max_frames
            .is_some_and(|limit| sessions[0].state.frames >= limit)
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
                let frame_started = Instant::now();
                handle_frame(
                    &mut sessions,
                    &options,
                    combined,
                    text.as_bytes(),
                    &mut unknown,
                )?;
                latency.record(frame_started.elapsed());
                update_live_book_combined(
                    &mut live_book,
                    &options,
                    combined,
                    text.as_bytes(),
                    &mut book_latency,
                    &mut book_updates,
                    &mut book_errors,
                );
                continue;
            }
            Ok(Message::Binary(bytes)) => {
                let frame_started = Instant::now();
                handle_frame(&mut sessions, &options, combined, &bytes, &mut unknown)?;
                latency.record(frame_started.elapsed());
                update_live_book_combined(
                    &mut live_book,
                    &options,
                    combined,
                    &bytes,
                    &mut book_latency,
                    &mut book_updates,
                    &mut book_errors,
                );
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
        // Fixed 250ms wait, deliberately not exponential: the recorder is
        // lossless-first, so a reconnect should cost a quarter-second hole,
        // not a growing one. An accumulating backoff would turn every late-
        // soak blip into a 30-second gap for no benefit — a failed reconnect
        // attempt already ends the capture outright (see below).
        thread::sleep(RECONNECT_INITIAL_BACKOFF);

        if interrupted.load(Ordering::SeqCst) {
            break STOP_INTERRUPTED.to_owned();
        }

        socket = match open_connection(&options) {
            Ok(socket) => socket,
            Err(error) => break format!("reconnect_failed: {error}"),
        };
        let reconnected_at = Timestamp::now();

        for session in sessions.iter_mut() {
            let gap_started = session.state.last_frame_at.unwrap_or(reconnected_at);
            session.append_gap(
                &options.instrument,
                gap_started,
                reconnected_at,
                reconnects_used,
                disconnect.gap_reason(),
            )?;
            session.state.connection_gaps += 1;
            session.tracker.reset();
        }
    };

    let mut outcomes = Vec::with_capacity(sessions.len());
    for session in sessions {
        outcomes.push(session.finish(&stop_reason)?);
    }
    let primary = outcomes.remove(0);

    Ok(CaptureOutcome {
        capture_id: primary.capture_id.clone(),
        frames_written: primary.frames_written,
        checked_frames: primary.checked_frames,
        connection_gaps: primary.connection_gaps,
        sequence_gaps: primary.sequence_gaps,
        stop_reason,
        latency: latency.summary().unwrap_or_default(),
        book_latency: book_latency.summary().unwrap_or_default(),
        book_updates,
        book_errors,
        fanouts: outcomes,
        unknown_frames: unknown.count,
        first_unknown_stream: unknown.first_stream,
    })
}

fn update_live_book_combined(
    book: &mut Reconstructor,
    options: &CaptureOptions,
    combined: bool,
    payload: &[u8],
    latency: &mut LatencyTracker,
    updates: &mut u64,
    errors: &mut u64,
) {
    if !combined {
        return update_live_book(
            book,
            &options.instrument,
            options.channel,
            payload,
            latency,
            updates,
            errors,
        );
    }

    let Some((stream, data)) = feed::unwrap_combined(payload) else {
        return;
    };
    let Some(channel) = feed::channel_for_stream(&stream) else {
        return;
    };
    update_live_book(
        book,
        &options.instrument,
        channel,
        &data,
        latency,
        updates,
        errors,
    );
}

fn handle_frame(
    sessions: &mut [Session],
    options: &CaptureOptions,
    combined: bool,
    payload: &[u8],
    unknown: &mut UnknownFrames,
) -> Result<(), RecordError> {
    if !combined {
        let session = &mut sessions[0];
        return append_frame(session, &options.instrument, payload);
    }

    let Some((stream, data)) = feed::unwrap_combined(payload) else {
        unknown.count += 1;
        return Ok(());
    };
    let Some(channel) = feed::channel_for_stream(&stream) else {
        unknown.count += 1;
        if unknown.first_stream.is_none() {
            unknown.first_stream = Some(stream);
        }
        return Ok(());
    };
    let Some(session) = sessions
        .iter_mut()
        .find(|session| session.channel == channel)
    else {
        unknown.count += 1;
        if unknown.first_stream.is_none() {
            unknown.first_stream = Some(stream);
        }
        return Ok(());
    };

    append_frame(session, &options.instrument, &data)
}

fn update_live_book(
    book: &mut Reconstructor,
    instrument: &Instrument,
    channel: Channel,
    payload: &[u8],
    latency: &mut LatencyTracker,
    updates: &mut u64,
    errors: &mut u64,
) {
    let venue = instrument.venue();

    if let Some(snapshot) = feed::inband_snapshot(venue, channel, payload) {
        let started = Instant::now();
        if book.load_snapshot(&snapshot).is_ok() {
            *updates += 1;
        } else {
            *errors += 1;
        }
        latency.record(started.elapsed());
        return;
    }

    let (Some(span), Some(diff)) = (
        feed::update_span(venue, channel, payload),
        feed::book_diff(venue, channel, payload),
    ) else {
        return;
    };

    let started = Instant::now();
    match book.apply_event(span, &diff) {
        Ok(astra_book::ApplyDecision::Applied) => *updates += 1,
        // Rejected and pre-snapshot events change nothing and carry no bad
        // data: they touch neither counter. Errors below are reserved for
        // invalid levels the venue should never have sent.
        Ok(_) => {}
        Err(_) => *errors += 1,
    }
    latency.record(started.elapsed());
}

fn open_connection(
    options: &CaptureOptions,
) -> Result<WebSocket<MaybeTlsStream<TcpStream>>, RecordError> {
    let (mut socket, _response) = connect(&options.url)?;
    set_read_timeout(&mut socket, READ_POLL)?;

    let mut channels = vec![options.channel];
    channels.extend(options.fanouts.iter().map(|fanout| fanout.channel));
    for channel in channels {
        if let Some(subscribe) = feed::subscribe_message(&options.instrument, channel) {
            socket.send(Message::text(subscribe))?;
        }
    }

    Ok(socket)
}

fn append_frame(
    session: &mut Session,
    instrument: &Instrument,
    payload: &[u8],
) -> Result<(), RecordError> {
    let span = feed::update_span(instrument.venue(), session.channel, payload);

    if let Some(reason) = session.tracker.check(span) {
        let now = Timestamp::now();
        let started_at = session.state.last_frame_at.unwrap_or(now);
        session.append_gap(instrument, started_at, now, 0, reason)?;
    }

    let record = CaptureRecord {
        seq: session.state.seq,
        instrument: instrument.clone(),
        channel: session.channel,
        ts_socket: Timestamp::now(),
        ts_exchange: None,
        payload: payload.to_vec(),
        flags: CaptureFlags::NONE,
    };

    session.writer.append(&record)?;
    session.state.seq += 1;
    session.state.frames += 1;
    session.state.last_frame_at = Some(record.ts_socket);

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
                close_cleanly(&mut socket);
            }
        });

        format!("ws://{address}")
    }

    fn close_cleanly(socket: &mut WebSocket<TcpStream>) {
        let _ = socket.close(None);
        let _ = socket
            .get_mut()
            .set_read_timeout(Some(Duration::from_millis(500)));
        while socket.read().is_ok() {}
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
        options_for(output, url, instrument())
    }

    fn options_for(output: &Path, url: String, instrument: Instrument) -> CaptureOptions {
        CaptureOptions {
            output: output.to_owned(),
            instrument,
            channel: Channel::BookDiff,
            url,
            max_frames: None,
            duration: None,
            max_reconnects: 0,
            fanouts: Vec::new(),
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

    fn wrapped(stream: &str, inner: &str) -> String {
        format!(r#"{{"stream":"{stream}","data":{inner}}}"#)
    }

    const SNAPSHOT_INNER: &str =
        r#"{"lastUpdateId":95,"bids":[["100.00000000","1"]],"asks":[["101.00000000","1"]]}"#;

    #[test]
    fn combined_capture_routes_each_stream_to_its_own_dir() {
        let diff = depth_frame(100, 110);
        let url = serve_connections(vec![vec![
            wrapped("btcusdt@depth@100ms", &diff),
            wrapped("btcusdt@depth10@100ms", SNAPSHOT_INNER),
            wrapped("btcusdt@kline_1m", &diff),
        ]]);
        let primary = temp_directory("combined-primary");
        let fanout_dir = temp_directory("combined-fanout");

        let mut opts = options(&primary, url);
        opts.fanouts = vec![Fanout {
            dir: fanout_dir.clone(),
            channel: Channel::BookSnapshot,
        }];
        let outcome = run_capture(opts, Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 1);
        assert_eq!(outcome.unknown_frames, 1);
        assert_eq!(
            outcome.first_unknown_stream.as_deref(),
            Some("btcusdt@kline_1m")
        );
        assert_eq!(outcome.fanouts.len(), 1);
        assert_eq!(outcome.fanouts[0].frames_written, 1);
        assert_eq!(outcome.fanouts[0].channel, Channel::BookSnapshot);

        let snap_records = store::read_all(&fanout_dir.join(FRAMES_DIR)).unwrap();
        assert_eq!(snap_records.len(), 1);
        assert_eq!(snap_records[0].payload, SNAPSHOT_INNER.as_bytes());

        std::fs::remove_dir_all(&primary).unwrap();
        std::fs::remove_dir_all(&fanout_dir).unwrap();
    }

    #[test]
    fn combined_capture_counts_unparseable_frames() {
        let url = serve_connections(vec![vec!["not json".to_owned()]]);
        let primary = temp_directory("combined-garbage");
        let fanout_dir = temp_directory("combined-garbage-fanout");

        let mut opts = options(&primary, url);
        opts.fanouts = vec![Fanout {
            dir: fanout_dir.clone(),
            channel: Channel::BookSnapshot,
        }];
        let outcome = run_capture(opts, Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 0);
        assert_eq!(outcome.unknown_frames, 1);
        assert_eq!(outcome.first_unknown_stream, None);
        assert_eq!(outcome.fanouts[0].frames_written, 0);

        std::fs::remove_dir_all(&primary).unwrap();
        std::fs::remove_dir_all(&fanout_dir).unwrap();
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
    fn saturated_update_ids_neither_panic_nor_emit_gaps() {
        // Hostile boundary: update IDs at u64::MAX. No forward jump is
        // representable past saturation, so the tracker must stay silent
        // instead of panicking on `previous + 1`.
        let mut tracker = SequenceTracker::default();
        assert!(
            tracker
                .check(Some(UpdateSpan::new(u64::MAX - 1, u64::MAX)))
                .is_none()
        );
        assert!(tracker.check(Some(UpdateSpan::new(0, 10))).is_none());
        assert_eq!(tracker.gaps, 0);
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
    fn latency_percentiles_follow_the_sorted_samples() {
        let mut tracker = LatencyTracker::default();
        assert_eq!(tracker.summary().map(|summary| summary.samples), None);

        for nanos in [100u64, 200, 300, 400, 500, 600, 700, 800, 900, 1000] {
            tracker.record(Duration::from_nanos(nanos));
        }

        let summary = tracker.summary().unwrap();
        assert_eq!(summary.samples, 10);
        assert_eq!(summary.p50_ns, 600);
        assert_eq!(summary.p99_ns, 1000);
        assert_eq!(summary.max_ns, 1000);
    }

    #[test]
    fn a_capture_reports_its_read_to_store_latency() {
        let url = serve_connections(vec![vec!["{}".to_owned(), "{}".to_owned()]]);
        let output = temp_directory("latency");
        let outcome = run_capture(options(&output, url), Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 2);
        assert_eq!(outcome.latency.samples, 2);
        assert!(outcome.latency.p50_ns > 0);
        assert!(outcome.latency.max_ns >= outcome.latency.p50_ns);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn rejected_book_events_touch_neither_counter() {
        // A venue sequence jump breaks the live book: the event is neither
        // a successful update nor invalid data. (The capture layer still
        // writes its own sequence-gap record for the jump.)
        let url = serve_connections(vec![vec![depth_frame(100, 110), depth_frame(200, 210)]]);
        let output = temp_directory("live-book-rejected");
        let outcome = run_capture(options(&output, url), Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 2);
        assert_eq!(outcome.sequence_gaps, 1);
        assert_eq!(outcome.book_updates, 1);
        assert_eq!(outcome.book_errors, 0);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn the_live_book_updates_as_frames_arrive() {
        let url = serve_connections(vec![vec![
            depth_frame(100, 110),
            depth_frame(111, 120),
            depth_frame(121, 130),
        ]]);
        let output = temp_directory("live-book");
        let outcome = run_capture(options(&output, url), Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 3);
        assert_eq!(outcome.book_updates, 3);
        assert_eq!(outcome.book_latency.samples, 3);
        assert!(outcome.book_latency.max_ns >= outcome.book_latency.p50_ns);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn live_book_failures_are_counted_not_hidden() {
        let bad_quantity = r#"{"e":"depthUpdate","s":"BTCUSDT","U":111,"u":120,"b":[["100.00000000","-1"]],"a":[]}"#.to_owned();
        let url = serve_connections(vec![vec![depth_frame(100, 110), bad_quantity]]);
        let output = temp_directory("live-book-errors");
        let outcome = run_capture(options(&output, url), Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 2);
        assert_eq!(outcome.sequence_gaps, 0);
        assert_eq!(outcome.book_updates, 1);
        assert_eq!(outcome.book_errors, 1);

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn capturing_into_a_used_directory_is_refused_not_merged() {
        let output = temp_directory("used-dir");
        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();
        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), 100).unwrap();
        writer
            .append(&CaptureRecord {
                seq: 0,
                instrument: instrument(),
                channel: Channel::BookDiff,
                ts_socket: Timestamp::from_unix_nanos(0),
                ts_exchange: None,
                payload: depth_frame(100, 110).into_bytes(),
                flags: CaptureFlags::NONE,
            })
            .unwrap();
        writer.finish().unwrap();

        assert!(matches!(
            Session::open(&output, &instrument(), Channel::BookDiff),
            Err(crate::error::RecordError::CaptureExists(_))
        ));
        // A fresh directory still opens: the guard targets chunks, not dirs.
        let fresh = temp_directory("fresh-dir");
        assert!(Session::open(&fresh, &instrument(), Channel::BookDiff).is_ok());

        std::fs::remove_dir_all(&output).unwrap();
        std::fs::remove_dir_all(&fresh).unwrap();
    }

    #[test]
    fn the_capture_report_format_is_pinned_line_by_line() {
        // ops/soak.sh status greps the frames line out of capture logs.
        // If this format moves, supervision breaks silently.
        let outcome = CaptureOutcome {
            capture_id: "test-id".to_owned(),
            frames_written: 601,
            checked_frames: 601,
            connection_gaps: 0,
            sequence_gaps: 0,
            stop_reason: STOP_DURATION.to_owned(),
            latency: LatencySummary {
                samples: 601,
                p50_ns: 108_000,
                p99_ns: 761_000,
                max_ns: 1_400_000,
            },
            book_latency: LatencySummary {
                samples: 601,
                p50_ns: 15_000,
                p99_ns: 68_000,
                max_ns: 100_000,
            },
            book_updates: 601,
            book_errors: 0,
            fanouts: vec![FanoutOutcome {
                capture_id: "fanout-id".to_owned(),
                channel: Channel::BookSnapshot,
                dir: PathBuf::from("./val-book_snapshot"),
                frames_written: 600,
                checked_frames: 0,
                connection_gaps: 0,
                sequence_gaps: 0,
            }],
            unknown_frames: 2,
            first_unknown_stream: Some("btcusdt@kline_1m".to_owned()),
        };
        let text = format_outcome(
            Path::new("./val"),
            "wss://example.invalid/ws",
            &instrument(),
            Channel::BookDiff,
            &outcome,
        );
        for line in [
            "capture_id  test-id",
            "output      ./val",
            "url         wss://example.invalid/ws",
            "instrument  binance spot BTC/USDT",
            "channel     book_diff",
            "frames      601",
            "checked     601",
            "conn_gaps   0",
            "seq_gaps    0",
            "fanout      ./val-book_snapshot book_snapshot frames=600 checked=0 gaps=0",
            "unknown     2 (first: btcusdt@kline_1m)",
            "latency_us  p50 108.0 p99 761.0 max 1400.0 (601 frames, read to stored)",
            "book_us     p50 15.0 p99 68.0 max 100.0 (updates 601, errors 0, read to book-updated)",
            "stop_reason duration_elapsed",
            "manifest    ./val/manifest.json",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }
    }

    #[test]
    fn the_init_report_format_is_pinned_line_by_line() {
        let manifest = CaptureManifest {
            schema_version: astra_types::SCHEMA_VERSION,
            capture_id: CaptureId::new("init-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument(),
            channel: Channel::BookDiff,
            frames_written: 0,
            stop_reason: Some(STOP_IN_PROGRESS.to_owned()),
        };
        let text = format_init_report(Path::new("./capture"), &manifest);
        for line in [
            "capture_id  init-test",
            "output      ./capture",
            "instrument  binance spot BTC/USDT",
            "channel     book_diff",
            "frames      0",
            "manifest    ./capture/manifest.json",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }
    }

    #[test]
    fn the_live_book_bootstraps_from_an_inband_snapshot() {
        let payload: &str = include_str!("../testdata/bybit_snapshot_deltas.json");
        let frames: Vec<serde_json::Value> = serde_json::from_str(payload).unwrap();
        let messages: Vec<String> = frames
            .into_iter()
            .take(2)
            .map(|frame| serde_json::to_string(&frame).unwrap())
            .collect();

        let url = serve_connections(vec![messages]);
        let output = temp_directory("live-book-bybit");
        let bybit = Instrument::new(
            Venue::Bybit,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        );
        let mut options = options_for(&output, url, bybit);
        options.max_frames = Some(2);
        let outcome = run_capture(options, Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 2);
        assert_eq!(outcome.stop_reason, STOP_MAX_FRAMES);
        assert_eq!(outcome.book_updates, 2);
        assert_eq!(outcome.book_latency.samples, 2);
        assert_eq!(outcome.sequence_gaps, 0);

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
