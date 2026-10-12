use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const SCALE: i128 = 100_000_000;
pub const DECIMAL_PLACES: u32 = 8;

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug, Serialize, Deserialize,
)]
#[serde(into = "String", try_from = "String")]
pub struct Fixed(i128);

#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum FixedParseError {
    #[error("empty decimal literal")]
    Empty,
    #[error("invalid decimal literal: {0}")]
    Invalid(String),
    #[error("decimal literal has more than {DECIMAL_PLACES} places: {0}")]
    ExcessPrecision(String),
    #[error("decimal literal out of range: {0}")]
    OutOfRange(String),
}

impl Fixed {
    pub const ZERO: Fixed = Fixed(0);
    pub const ONE: Fixed = Fixed(SCALE);

    pub const fn from_raw(raw: i128) -> Self {
        Fixed(raw)
    }

    pub const fn raw(self) -> i128 {
        self.0
    }

    pub const fn from_units(units: i64) -> Self {
        Fixed(units as i128 * SCALE)
    }

    pub fn checked_add(self, rhs: Self) -> Option<Self> {
        self.0.checked_add(rhs.0).map(Fixed)
    }

    pub fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.0.checked_sub(rhs.0).map(Fixed)
    }

    pub fn checked_mul(self, rhs: Self) -> Option<Self> {
        round_div(self.0.checked_mul(rhs.0)?, SCALE).map(Fixed)
    }

    pub fn checked_div(self, rhs: Self) -> Option<Self> {
        if rhs.0 == 0 {
            return None;
        }
        round_div(self.0.checked_mul(SCALE)?, rhs.0).map(Fixed)
    }

    pub fn checked_neg(self) -> Option<Self> {
        self.0.checked_neg().map(Fixed)
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn to_f64_for_analytics(self) -> f64 {
        self.0 as f64 / SCALE as f64
    }
}

impl From<Fixed> for String {
    fn from(value: Fixed) -> Self {
        value.to_string()
    }
}

impl TryFrom<String> for Fixed {
    type Error = FixedParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl FromStr for Fixed {
    type Err = FixedParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(FixedParseError::Empty);
        }

        let (negative, digits) = match trimmed.as_bytes()[0] {
            b'-' => (true, &trimmed[1..]),
            b'+' => (false, &trimmed[1..]),
            _ => (false, trimmed),
        };

        let (whole, fraction) = match digits.split_once('.') {
            Some((whole, fraction)) => (whole, fraction),
            None => (digits, ""),
        };

        if fraction.len() > DECIMAL_PLACES as usize {
            return Err(FixedParseError::ExcessPrecision(trimmed.to_owned()));
        }

        let whole = if whole.is_empty() { "0" } else { whole };
        if !whole.bytes().all(|b| b.is_ascii_digit())
            || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(FixedParseError::Invalid(trimmed.to_owned()));
        }

        let mut units: i128 = whole
            .parse()
            .map_err(|_| FixedParseError::OutOfRange(trimmed.to_owned()))?;
        units = units
            .checked_mul(SCALE)
            .ok_or_else(|| FixedParseError::OutOfRange(trimmed.to_owned()))?;

        let padded = format!("{fraction:0<8}");
        let frac: i128 = padded
            .parse()
            .map_err(|_| FixedParseError::OutOfRange(trimmed.to_owned()))?;

        let magnitude = units
            .checked_add(frac)
            .ok_or_else(|| FixedParseError::OutOfRange(trimmed.to_owned()))?;

        Ok(Fixed(if negative { -magnitude } else { magnitude }))
    }
}

impl fmt::Display for Fixed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let magnitude = self.0.unsigned_abs();
        let scale = SCALE as u128;
        let units = magnitude / scale;
        let fraction = magnitude % scale;
        write!(f, "{sign}{units}.{fraction:08}")
    }
}

fn round_div(numerator: i128, denominator: i128) -> Option<i128> {
    // Sign-normalize first: with a negative divisor the truncation below
    // rounds ties toward zero instead of away. Negation can itself overflow
    // (i128::MIN), and that fails loud like every other checked op.
    let (numerator, denominator) = if denominator < 0 {
        (numerator.checked_neg()?, denominator.checked_neg()?)
    } else {
        (numerator, denominator)
    };
    let half = denominator / 2;
    if numerator >= 0 {
        numerator.checked_add(half)?.checked_div(denominator)
    } else {
        numerator.checked_sub(half)?.checked_div(denominator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Fixed {
        s.parse().unwrap()
    }

    #[test]
    fn parses_and_displays_canonically() {
        assert_eq!(parse("0").to_string(), "0.00000000");
        assert_eq!(parse("108105.12").to_string(), "108105.12000000");
        assert_eq!(parse("-1.5").to_string(), "-1.50000000");
        assert_eq!(parse(".5").to_string(), "0.50000000");
        assert_eq!(parse("5.").to_string(), "5.00000000");
        assert_eq!(parse("+2.25").to_string(), "2.25000000");
    }

    #[test]
    fn rejects_bad_literals() {
        assert_eq!("".parse::<Fixed>().unwrap_err(), FixedParseError::Empty);
        assert_eq!(
            "1.234567891".parse::<Fixed>().unwrap_err(),
            FixedParseError::ExcessPrecision("1.234567891".to_owned())
        );
        assert_eq!(
            "12.a".parse::<Fixed>().unwrap_err(),
            FixedParseError::Invalid("12.a".to_owned())
        );
        assert_eq!(
            "1.2.3".parse::<Fixed>().unwrap_err(),
            FixedParseError::Invalid("1.2.3".to_owned())
        );
    }

    #[test]
    fn rounds_half_away_from_zero_at_the_eighth_place() {
        assert_eq!(
            parse("1.5")
                .checked_mul(parse("0.00000001"))
                .unwrap()
                .to_string(),
            "0.00000002"
        );
        assert_eq!(
            parse("-1.5")
                .checked_mul(parse("0.00000001"))
                .unwrap()
                .to_string(),
            "-0.00000002"
        );
        assert_eq!(
            parse("2").checked_div(parse("3")).unwrap().to_string(),
            "0.66666667"
        );
    }

    #[test]
    fn arithmetic_is_exact_at_scale() {
        let price = parse("108105.12");
        let quantity = parse("0.001");
        assert_eq!(
            price.checked_mul(quantity).unwrap().to_string(),
            "108.10512000"
        );
        assert_eq!(
            parse("2.5").checked_add(parse("0.25")).unwrap().to_string(),
            "2.75000000"
        );
        assert_eq!(
            parse("2.5").checked_sub(parse("3.0")).unwrap().to_string(),
            "-0.50000000"
        );
        assert_eq!(
            parse("1").checked_div(parse("8")).unwrap().to_string(),
            "0.12500000"
        );
        assert_eq!(parse("1").checked_div(Fixed::ZERO), None);
    }

    #[test]
    fn serialises_as_a_decimal_string() {
        let value = parse("-108105.12345678");
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "\"-108105.12345678\"");
        assert_eq!(serde_json::from_str::<Fixed>(&json).unwrap(), value);
    }

    /// Fixed-seed property sweep (strategy §9). No property framework is
    /// available offline, so a xorshift64 with a fixed seed drives thousands
    /// of algebraic checks deterministically — same seed, same cases, every
    /// run. Values stay within ±1e12 raw (±10k units, the realistic price
    /// band) so every cross-multiplied bound stays inside i128 and every
    /// check actually verifies instead of skipping on overflow.
    fn sweep_state() -> u64 {
        0x9E37_79B9_7F4A_7C15
    }

    fn rng_next(state: &mut u64) -> u64 {
        let mut x = *state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        x
    }

    fn sweep_raw(state: &mut u64) -> i128 {
        match rng_next(state) % 16 {
            0 => 0,
            1 => 1,
            2 => -1,
            3 => 100_000_000,
            4 => -100_000_000,
            _ => {
                let magnitude = (rng_next(state) % 1_000_000_000_001) as i128;
                if rng_next(state) % 2 == 0 {
                    magnitude
                } else {
                    -magnitude
                }
            }
        }
    }

    #[test]
    fn addition_is_exact_and_invertible() {
        let mut state = sweep_state();
        for _ in 0..4096 {
            let (a, b) = (sweep_raw(&mut state), sweep_raw(&mut state));
            let (fa, fb) = (Fixed::from_raw(a), Fixed::from_raw(b));
            // Exactness against the integer op: None exactly when i128 says so.
            assert_eq!(
                fa.checked_add(fb).map(|sum| sum.raw()),
                a.checked_add(b),
                "{a} + {b}"
            );
            // Commutativity and invertibility where defined.
            assert_eq!(fa.checked_add(fb), fb.checked_add(fa));
            if let Some(sum) = fa.checked_add(fb) {
                assert_eq!(sum.checked_sub(fb), Some(fa), "({a} + {b}) - {b}");
            }
        }
    }

    #[test]
    fn multiplication_stays_within_half_ulp() {
        let mut state = sweep_state();
        for _ in 0..4096 {
            let (a, b) = (sweep_raw(&mut state), sweep_raw(&mut state));
            let Some(result) = Fixed::from_raw(a).checked_mul(Fixed::from_raw(b)) else {
                panic!("sweep range must stay multipliable: {a} * {b}");
            };
            // |r*S - a*b| * 2 <= S: the rounded result is the nearest raw
            // unit to the exact rational, ties included.
            let error = (result.raw() * SCALE - a * b).abs();
            assert!(
                error * 2 <= SCALE,
                "{a} * {b} = {} off by {error}",
                result.raw()
            );
        }
    }

    #[test]
    fn division_stays_within_half_ulp_on_every_divisor_sign() {
        let mut state = sweep_state();
        let mut covered_nonzero = 0u32;
        for _ in 0..8192 {
            let (a, b) = (sweep_raw(&mut state), sweep_raw(&mut state));
            if b == 0 {
                continue;
            }
            covered_nonzero += 1;
            let Some(result) = Fixed::from_raw(a).checked_div(Fixed::from_raw(b)) else {
                panic!("sweep range must stay divisible: {a} / {b}");
            };
            // |r*b - a*S| * 2 <= |b|, for negative divisors too — this is the
            // property the sign-normalized round_div exists to guarantee.
            let error = (result.raw() * b - a * SCALE).abs();
            assert!(
                error * 2 <= b.abs(),
                "{a} / {b} = {} off by {error}",
                result.raw()
            );
        }
        assert!(covered_nonzero > 7000, "sweep must exercise division");
    }

    #[test]
    fn negation_is_an_exact_sign_flip() {
        let mut state = sweep_state();
        for _ in 0..4096 {
            let (a, b) = (sweep_raw(&mut state), sweep_raw(&mut state));
            let (fa, fb) = (Fixed::from_raw(a), Fixed::from_raw(b));
            // Rounding is an odd function on every divisor sign: negating an
            // input negates the output exactly, never by a neighboring ulp.
            assert_eq!(
                fa.checked_mul(fb).and_then(|p| p.checked_neg()),
                Fixed::from_raw(a)
                    .checked_mul(Fixed::from_raw(-b))
                    .or_else(|| Fixed::from_raw(-a).checked_mul(fb)),
                "symmetry of {a} * {b}"
            );
            assert_eq!(
                fa.checked_div(fb).and_then(|q| q.checked_neg()),
                if b == 0 {
                    None
                } else {
                    Fixed::from_raw(-a).checked_div(fb)
                },
                "symmetry of {a} / {b}"
            );
        }
        assert_eq!(Fixed::from_raw(i128::MIN).checked_neg(), None);
        assert_eq!(
            Fixed::from_raw(i128::MAX).checked_add(Fixed::from_raw(1)),
            None
        );
    }

    #[test]
    fn display_parse_round_trips_on_sweep_values() {
        let mut state = sweep_state();
        for _ in 0..1024 {
            let value = Fixed::from_raw(sweep_raw(&mut state));
            assert_eq!(value.to_string().parse::<Fixed>().unwrap(), value);
        }
    }

    #[test]
    fn f64_projection_preserves_order_and_sign() {
        let mut state = sweep_state();
        for _ in 0..1024 {
            let (a, b) = (sweep_raw(&mut state), sweep_raw(&mut state));
            let (fa, fb) = (
                Fixed::from_raw(a).to_f64_for_analytics(),
                Fixed::from_raw(b).to_f64_for_analytics(),
            );
            assert_eq!(fa < fb, a < b, "{a} vs {b}");
            assert_eq!(fa == 0.0, a == 0, "{a}");
            assert_eq!(fa > 0.0, a > 0, "{a}");
        }
    }

    #[test]
    fn ties_round_away_from_zero_whichever_side_the_divisor_is_on() {
        // Exact half-ulp quotients: 1 / 2.0 is +0.5 raw either way you
        // write the divisor, so both must land on +1 raw.
        assert_eq!(
            Fixed::from_raw(1).checked_div(Fixed::from_raw(200_000_000)),
            Some(Fixed::from_raw(1))
        );
        assert_eq!(
            Fixed::from_raw(1).checked_div(Fixed::from_raw(-200_000_000)),
            Some(Fixed::from_raw(-1))
        );
        assert_eq!(
            Fixed::from_raw(-1).checked_div(Fixed::from_raw(-200_000_000)),
            Some(Fixed::from_raw(1))
        );
        assert_eq!(
            Fixed::from_raw(-1).checked_div(Fixed::from_raw(200_000_000)),
            Some(Fixed::from_raw(-1))
        );
        // Multiplication ties behave the same through the shared divider.
        assert_eq!(
            Fixed::from_raw(1).checked_mul(Fixed::from_raw(50_000_000)),
            Some(Fixed::from_raw(1))
        );
        assert_eq!(
            Fixed::from_raw(-1).checked_mul(Fixed::from_raw(50_000_000)),
            Some(Fixed::from_raw(-1))
        );
    }
}
