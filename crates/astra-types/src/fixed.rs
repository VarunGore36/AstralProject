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
}
