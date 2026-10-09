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

#[pymodule]
mod astra {
    #[pymodule_export]
    use super::{fixed_add, fixed_div, fixed_mul, fixed_sub, parse_fixed, timestamp_now_ns};
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
}
