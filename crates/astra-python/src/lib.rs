//! Python bindings foothold (`astra` module): fixed-point decimals and
//! timestamps for the research layer.
//!
//! Deliberately small: parse, canonical display, exact arithmetic, and clock
//! reads. DataFrames stay on the Python side (Polars/DuckDB over the Parquet
//! this project writes); this crate keeps money math out of floats.
//!
//! Tested through the embedded interpreter (`cargo test`). Importable
//! packaging (maturin) is a later distribution push, stated openly —
//! `import astra` does not work yet.

use astra_types::{Fixed, Timestamp};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

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

#[pymodule]
mod astra {
    #[pymodule_export]
    use super::{
        fixed_add, fixed_div, fixed_mul, fixed_sub, parse_fixed, simulate_fill, timestamp_now_ns,
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
}
