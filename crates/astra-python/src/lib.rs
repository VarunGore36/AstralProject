//! Python bindings foothold (`astra` module): fixed-point decimals and
//! timestamps for the research layer.
//!
//! Deliberately small: parse, canonical display, exact arithmetic, and clock
//! reads. DataFrames stay on the Python side (Polars/DuckDB over the Parquet
//! this project writes); this crate keeps money math out of floats.
//!
//! Tested through the embedded interpreter (`cargo test`) and shipped as
//! a real wheel (`maturin build` in `crates/astra-python`, CI-verified
//! `pip install` plus import smoke test). `import astra` works from the
//! installed wheel; local development can also use the plain `cargo build`
//! `astra.so` without maturin.

use astra_types::{Fixed, Timestamp};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

fn parse_fixed_arg(s: &str) -> PyResult<Fixed> {
    s.parse()
        .map_err(|error| PyValueError::new_err(format!("bad decimal {s:?}: {error}")))
}

fn render(value: Option<Fixed>, what: &str) -> PyResult<String> {
    value
        .map(|value| value.to_string())
        .ok_or_else(|| PyValueError::new_err(what.to_owned()))
}

/// Parse a decimal literal into canonical eight-place form.
#[pyfunction]
fn parse_fixed(s: &str) -> PyResult<String> {
    Ok(parse_fixed_arg(s)?.to_string())
}

/// Exact arithmetic. Overflow (and division by zero) raise, never wrap.
#[pyfunction]
fn fixed_add(a: &str, b: &str) -> PyResult<String> {
    render(
        parse_fixed_arg(a)?.checked_add(parse_fixed_arg(b)?),
        "fixed-point addition overflowed",
    )
}

/// Exact arithmetic. Overflow raises, never wraps.
#[pyfunction]
fn fixed_sub(a: &str, b: &str) -> PyResult<String> {
    render(
        parse_fixed_arg(a)?.checked_sub(parse_fixed_arg(b)?),
        "fixed-point subtraction overflowed",
    )
}

/// Exact multiplication, half away from zero at the eighth place.
#[pyfunction]
fn fixed_mul(a: &str, b: &str) -> PyResult<String> {
    render(
        parse_fixed_arg(a)?.checked_mul(parse_fixed_arg(b)?),
        "fixed-point multiplication overflowed",
    )
}

/// Exact division, half away from zero. Zero divisors raise.
#[pyfunction]
fn fixed_div(a: &str, b: &str) -> PyResult<String> {
    render(
        parse_fixed_arg(a)?.checked_div(parse_fixed_arg(b)?),
        "fixed-point division overflowed or divided by zero",
    )
}

/// Current wall-clock time as integer nanoseconds since the epoch.
#[pyfunction]
fn timestamp_now_ns() -> PyResult<i64> {
    Ok(Timestamp::now().unix_nanos())
}

/// Simulate one resting limit order over trade prints.
///
/// `side` is "buy" or "sell"; prices and quantities are decimal strings;
/// `prints` is a list of `(seq, price, quantity)` tuples in stream order.
/// Returns the outcome as JSON (`{"result": "filled", ...}` or
/// `{"result": "unfilled", ...}`), serialised exactly like benchmark reports.
/// Gaps cannot be expressed here — pass gap-bearing streams through the
/// replay-backed probe or harness instead; this function answers the
/// print math and nothing else.
#[pyfunction]
fn simulate_fill(
    side: &str,
    price: &str,
    quantity: &str,
    fee_bps: u32,
    prints: Vec<(u64, String, String)>,
) -> PyResult<String> {
    use astra_exec::{LimitOrder, MarketEvent, Print, Side};
    use std::str::FromStr;

    let side = match side.to_ascii_lowercase().as_str() {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        _ => {
            return Err(PyValueError::new_err(format!(
                "side must be buy or sell, saw {side:?}"
            )));
        }
    };
    let parse = |value: &str| {
        Fixed::from_str(value)
            .map_err(|error| PyValueError::new_err(format!("bad decimal {value:?}: {error}")))
    };
    let order = LimitOrder {
        side,
        price: parse(price)?,
        quantity: parse(quantity)?,
    };
    let mut events = Vec::with_capacity(prints.len());
    for (seq, price, quantity) in &prints {
        events.push(MarketEvent::Print(Print {
            seq: *seq,
            price: parse(price)?,
            quantity: parse(quantity)?,
        }));
    }
    let outcome = astra_exec::simulate(&order, fee_bps, &events)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serde_json::to_string(&outcome).map_err(|error| PyValueError::new_err(error.to_string()))
}

/// Normalize a capture directory into Hive-partitioned Parquet.
///
/// Returns a dict with `records`, `rows_written`,
/// `skipped_unsupported_channel`, and `files` (final part paths). Any
/// Rust-side failure raises `ValueError` with the engine's message —
/// including torn manifests and validation rejections.
#[pyfunction]
fn normalize_capture<'py>(
    py: Python<'py>,
    input: &str,
    output: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let summary =
        astra_normalize::normalize(std::path::Path::new(input), std::path::Path::new(output))
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let dict = PyDict::new(py);
    dict.set_item("records", summary.records)?;
    dict.set_item("rows_written", summary.rows_written)?;
    dict.set_item(
        "skipped_unsupported_channel",
        summary.skipped_unsupported_channel,
    )?;
    let files: Vec<String> = summary
        .files
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    dict.set_item("files", files)?;
    Ok(dict)
}

/// Replay a capture through a built-in strategy (`"book-top"` or
/// `"trade-tally"`) and return the report as a dict. Custom strategies stay
/// in Rust for now; this covers the full record → normalize → replay loop
/// from a notebook.
#[pyfunction]
fn replay_capture<'py>(
    py: Python<'py>,
    capture: &str,
    seed: u64,
    strategy: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let report = run_with(strategy, |strategy| {
        astra_replay::replay(std::path::Path::new(capture), seed, strategy)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    })?;
    report_dict(py, &report)
}

/// Replay a normalized Parquet tree through a built-in strategy. Null rows
/// replay as counted skips — the same answer as raw replay, proven by the
/// `raw_and_normalized_replay_emit_identical_signals` test.
#[pyfunction]
fn replay_tables<'py>(
    py: Python<'py>,
    tables: &str,
    seed: u64,
    strategy: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let report = run_with(strategy, |strategy| {
        astra_replay::replay_normalized(std::path::Path::new(tables), seed, strategy)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    })?;
    report_dict(py, &report)
}

fn run_with(
    name: &str,
    run: impl FnOnce(&mut dyn astra_replay::Strategy) -> PyResult<astra_replay::ReplayReport>,
) -> PyResult<astra_replay::ReplayReport> {
    match name {
        "book-top" => run(&mut astra_replay::BookTop::new()),
        "trade-tally" => run(&mut astra_replay::TradeTally::new()),
        _ => Err(PyValueError::new_err(format!(
            "strategy must be \"book-top\" or \"trade-tally\", saw {name:?}"
        ))),
    }
}

fn report_dict<'py>(
    py: Python<'py>,
    report: &astra_replay::ReplayReport,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("frames", report.frames)?;
    dict.set_item("events", report.events_emitted)?;
    dict.set_item("trades", report.trade_events)?;
    dict.set_item("topbooks", report.top_book_events)?;
    dict.set_item("gaps", report.gaps)?;
    dict.set_item(
        "skipped",
        report.skipped_channel + report.skipped_unparseable,
    )?;
    dict.set_item("signals", report.signals)?;
    dict.set_item("signal_hash", report.signal_hash.clone())?;
    Ok(dict)
}

#[pymodule]
mod astra {
    #[pymodule_export]
    use super::{
        fixed_add, fixed_div, fixed_mul, fixed_sub, normalize_capture, parse_fixed, replay_capture,
        replay_tables, simulate_fill, timestamp_now_ns,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attached<T>(f: impl FnOnce(Python<'_>) -> T) -> T {
        Python::initialize();
        Python::attach(f)
    }

    #[test]
    fn decimals_round_trip_canonically() {
        attached(|py| {
            let parse = pyo3::wrap_pyfunction!(parse_fixed, py).unwrap();
            let add = pyo3::wrap_pyfunction!(fixed_add, py).unwrap();
            let mul = pyo3::wrap_pyfunction!(fixed_mul, py).unwrap();
            let out: String = parse.call1(("108105.12",)).unwrap().extract().unwrap();
            assert_eq!(out, "108105.12000000");
            let out: String = add.call1(("2.5", "0.25")).unwrap().extract().unwrap();
            assert_eq!(out, "2.75000000");
            let out: String = mul
                .call1(("108105.12", "0.001"))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(out, "108.10512000");
        });
    }

    #[test]
    fn bad_literals_and_overflow_raise() {
        attached(|py| {
            let parse = pyo3::wrap_pyfunction!(parse_fixed, py).unwrap();
            let div = pyo3::wrap_pyfunction!(fixed_div, py).unwrap();
            assert!(parse.call1(("abc",)).is_err());
            assert!(div.call1(("1", "0")).is_err());
        });
    }

    #[test]
    fn clock_reads_monotonically() {
        attached(|py| {
            let now = pyo3::wrap_pyfunction!(timestamp_now_ns, py).unwrap();
            let first: i64 = now.call0().unwrap().extract().unwrap();
            let second: i64 = now.call0().unwrap().extract().unwrap();
            assert!(second >= first);
            assert!(first > 1_700_000_000_000_000_000);
        });
    }

    #[test]
    fn fills_simulate_from_python() {
        attached(|py| {
            let simulate = pyo3::wrap_pyfunction!(simulate_fill, py).unwrap();
            let prints = vec![
                (0u64, "101.00000000".to_owned(), "1".to_owned()),
                (1u64, "99.00000000".to_owned(), "1".to_owned()),
            ];
            let out: String = simulate
                .call1(("buy", "100.00000000", "1", 5u32, prints.clone()))
                .unwrap()
                .extract()
                .unwrap();
            assert!(out.contains("\"result\":\"filled\""), "{out}");
            assert!(out.contains("\"print_seq\":1"), "{out}");

            let out: String = simulate
                .call1(("sell", "102.00000000", "1", 5u32, prints))
                .unwrap()
                .extract()
                .unwrap();
            assert!(out.contains("\"result\":\"unfilled\""), "{out}");

            assert!(
                simulate
                    .call1((
                        "hold",
                        "100.00000000",
                        "1",
                        5u32,
                        Vec::<(u64, String, String)>::new()
                    ))
                    .is_err()
            );
        });
    }

    const DEPTH_FRAME: &str = r#"{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":100,"u":105,"b":[["108105.12000000","0.00100000"]],"a":[["108105.13000000","0.00200000"]]}"#;
    const NEXT_FRAME: &str = r#"{"e":"depthUpdate","E":1700000000100,"s":"BTCUSDT","U":106,"u":110,"b":[["108105.11000000","0"]],"a":[]}"#;

    fn write_capture(output: &std::path::Path) {
        use astra_record::capture::{FRAMES_DIR, write_manifest};
        use astra_record::store::ChunkWriter;
        use astra_types::{
            CaptureFlags, CaptureId, CaptureManifest, Channel, Instrument, MarketType, Symbol,
            Venue,
        };

        let instrument = Instrument::new(
            Venue::Binance,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        );
        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();
        let mut writer = ChunkWriter::open(output.join(FRAMES_DIR), 100).unwrap();
        for (seq, payload) in [DEPTH_FRAME, NEXT_FRAME].iter().enumerate() {
            writer
                .append(&astra_types::CaptureRecord {
                    seq: seq as u64,
                    instrument: instrument.clone(),
                    channel: Channel::BookDiff,
                    ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000 + seq as i64),
                    ts_exchange: None,
                    payload: payload.as_bytes().to_vec(),
                    flags: CaptureFlags::NONE,
                })
                .unwrap();
        }
        writer.finish().unwrap();
        write_manifest(
            output,
            &CaptureManifest {
                schema_version: astra_types::SCHEMA_VERSION,
                capture_id: CaptureId::new("python-test"),
                created_at: Timestamp::from_unix_nanos(0),
                instrument,
                channel: Channel::BookDiff,
                frames_written: 2,
                stop_reason: None,
            },
        )
        .unwrap();
    }

    #[test]
    fn the_full_loop_runs_from_python() {
        fn get_u64(dict: &Bound<'_, PyDict>, key: &str) -> u64 {
            dict.get_item(key).unwrap().unwrap().extract().unwrap()
        }

        attached(|py| {
            let capture =
                std::env::temp_dir().join(format!("astra-py-loop-{}", std::process::id()));
            let tables =
                std::env::temp_dir().join(format!("astra-py-loop-out-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&capture);
            let _ = std::fs::remove_dir_all(&tables);
            write_capture(&capture);

            let normalize = pyo3::wrap_pyfunction!(normalize_capture, py).unwrap();
            let report: Bound<'_, PyDict> = normalize
                .call1((capture.to_str().unwrap(), tables.to_str().unwrap()))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(get_u64(&report, "records"), 2);
            assert_eq!(get_u64(&report, "rows_written"), 2);

            let replay = pyo3::wrap_pyfunction!(replay_capture, py).unwrap();
            let raw: Bound<'_, PyDict> = replay
                .call1((capture.to_str().unwrap(), 7u64, "book-top"))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(get_u64(&raw, "frames"), 2);
            assert_eq!(get_u64(&raw, "signals"), 2);
            let raw_hash: String = raw
                .get_item("signal_hash")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(raw_hash.len(), 64);

            let retable = pyo3::wrap_pyfunction!(replay_tables, py).unwrap();
            let norm: Bound<'_, PyDict> = retable
                .call1((tables.to_str().unwrap(), 7u64, "book-top"))
                .unwrap()
                .extract()
                .unwrap();
            let norm_hash: String = norm
                .get_item("signal_hash")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(raw_hash, norm_hash);

            assert!(
                replay
                    .call1((capture.to_str().unwrap(), 7u64, "oracle"))
                    .is_err()
            );

            std::fs::remove_dir_all(&capture).unwrap();
            std::fs::remove_dir_all(&tables).unwrap();
        });
    }
}
