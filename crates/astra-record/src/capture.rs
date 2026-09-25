use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use astra_types::{
    CaptureFlags, CaptureId, CaptureManifest, CaptureRecord, Channel, Instrument, SCHEMA_VERSION,
    Timestamp,
};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, connect};

use crate::error::RecordError;
use crate::store::ChunkWriter;

pub const MANIFEST_FILE: &str = "manifest.json";
pub const FRAMES_DIR: &str = "frames";
pub const RECORDS_PER_CHUNK: usize = 2_000;
pub const READ_POLL: Duration = Duration::from_millis(250);

pub const STOP_IN_PROGRESS: &str = "in_progress";
pub const STOP_INTERRUPTED: &str = "interrupted";
pub const STOP_MAX_FRAMES: &str = "max_frames_reached";
pub const STOP_DURATION: &str = "duration_elapsed";
pub const STOP_VENUE_CLOSED: &str = "connection_closed_by_venue";

#[derive(Clone, Debug)]
pub struct CaptureOptions {
    pub output: PathBuf,
    pub instrument: Instrument,
    pub channel: Channel,
    pub url: String,
    pub max_frames: Option<u64>,
    pub duration: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct CaptureOutcome {
    pub capture_id: String,
    pub frames_written: u64,
    pub stop_reason: String,
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

pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub fn run_capture(
    options: CaptureOptions,
    interrupted: Arc<AtomicBool>,
) -> Result<CaptureOutcome, RecordError> {
    install_crypto_provider();

    let mut manifest = init_capture(&options.output, &options.instrument, options.channel)?;
    let mut writer = ChunkWriter::open(options.output.join(FRAMES_DIR), RECORDS_PER_CHUNK)?;

    let (mut socket, _response) = connect(&options.url)?;
    set_read_timeout(&mut socket, READ_POLL)?;

    let mut frames: u64 = 0;
    let started = Instant::now();

    let stop_reason = loop {
        if interrupted.load(Ordering::SeqCst) {
            break STOP_INTERRUPTED.to_owned();
        }
        if options.max_frames.is_some_and(|limit| frames >= limit) {
            break STOP_MAX_FRAMES.to_owned();
        }
        if options
            .duration
            .is_some_and(|limit| started.elapsed() >= limit)
        {
            break STOP_DURATION.to_owned();
        }

        match socket.read() {
            Ok(Message::Text(text)) => {
                append_frame(&mut writer, &options, frames, text.as_bytes())?;
                frames += 1;
            }
            Ok(Message::Binary(bytes)) => {
                append_frame(&mut writer, &options, frames, &bytes)?;
                frames += 1;
            }
            Ok(Message::Close(_)) => break STOP_VENUE_CLOSED.to_owned(),
            Ok(_) => {}
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => break format!("read_error: {error}"),
        }
    };

    writer.finish()?;

    manifest.frames_written = frames;
    manifest.stop_reason = Some(stop_reason.clone());
    write_manifest(&options.output, &manifest)?;

    Ok(CaptureOutcome {
        capture_id: manifest.capture_id.as_str().to_owned(),
        frames_written: frames,
        stop_reason,
    })
}

fn append_frame(
    writer: &mut ChunkWriter,
    options: &CaptureOptions,
    seq: u64,
    payload: &[u8],
) -> Result<(), RecordError> {
    let record = CaptureRecord {
        seq,
        instrument: options.instrument.clone(),
        channel: options.channel,
        ts_socket: Timestamp::now(),
        ts_exchange: None,
        payload: payload.to_vec(),
        flags: CaptureFlags::NONE,
    };

    writer.append(&record)?;

    Ok(())
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
    use std::thread;

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

    fn serve(frames: Vec<String>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                if let Ok(mut socket) = tungstenite::accept(stream) {
                    for frame in frames {
                        let _ = socket.send(Message::text(frame));
                    }
                    thread::sleep(Duration::from_millis(50));
                    let _ = socket.close(None);
                }
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
        }
    }

    fn read_manifest(output: &Path) -> CaptureManifest {
        let body = std::fs::read_to_string(output.join(MANIFEST_FILE)).unwrap();
        serde_json::from_str(&body).unwrap()
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
        let url = serve(vec![
            "{\"a\":1}".to_owned(),
            "héllo".to_owned(),
            String::new(),
        ]);
        let output = temp_directory("payloads");

        let outcome = run_capture(options(&output, url), Arc::new(AtomicBool::new(false))).unwrap();

        assert_eq!(outcome.frames_written, 3);
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
    fn max_frames_stops_the_capture_cleanly() {
        let url = serve((0..10).map(|i| format!("frame-{i}")).collect());
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
        let url = serve(vec!["{}".to_owned()]);
        let output = temp_directory("manifest");

        run_capture(options(&output, url), Arc::new(AtomicBool::new(false))).unwrap();

        let manifest = read_manifest(&output);
        assert_eq!(manifest.frames_written, 1);
        assert_eq!(manifest.stop_reason.as_deref(), Some(STOP_VENUE_CLOSED));

        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn interruption_stops_before_capturing() {
        let url = serve(vec!["{}".to_owned()]);
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
