use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    pub const fn from_unix_nanos(nanos: i64) -> Self {
        Timestamp(nanos)
    }

    pub const fn unix_nanos(self) -> i64 {
        self.0
    }

    pub fn now() -> Self {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Timestamp(i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX))
    }

    pub fn checked_duration_since(self, earlier: Self) -> Option<i64> {
        self.0.checked_sub(earlier.0)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let magnitude = self.0.unsigned_abs();
        let seconds = magnitude / 1_000_000_000;
        let nanos = magnitude % 1_000_000_000;
        write!(f, "{sign}{seconds}.{nanos:09}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_chronologically() {
        let first = Timestamp::from_unix_nanos(1_000);
        let second = Timestamp::from_unix_nanos(2_000);
        assert!(first < second);
        assert_eq!(second.checked_duration_since(first), Some(1_000));
    }

    #[test]
    fn formats_seconds_and_nanos() {
        assert_eq!(
            Timestamp::from_unix_nanos(1_500_000_000).to_string(),
            "1.500000000"
        );
        assert_eq!(Timestamp::from_unix_nanos(0).to_string(), "0.000000000");
    }

    #[test]
    fn serialises_as_nanos() {
        let value = Timestamp::from_unix_nanos(1_700_000_000_000_000_000);
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "1700000000000000000");
        assert_eq!(serde_json::from_str::<Timestamp>(&json).unwrap(), value);
    }
}
