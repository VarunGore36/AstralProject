use serde::{Deserialize, Serialize};

use crate::instrument::{Channel, Instrument};
use crate::time::Timestamp;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CaptureId(String);

impl CaptureId {
    pub fn new(value: impl Into<String>) -> Self {
        CaptureId(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CaptureFlags(u32);

impl CaptureFlags {
    pub const NONE: Self = CaptureFlags(0);
    pub const SEQUENCE_GAP: Self = CaptureFlags(1 << 0);
    pub const DUPLICATE: Self = CaptureFlags(1 << 1);
    pub const RESYNC: Self = CaptureFlags(1 << 2);
    pub const STALE: Self = CaptureFlags(1 << 3);
    pub const UNRELIABLE: Self = CaptureFlags(1 << 4);
    pub const TRUNCATED: Self = CaptureFlags(1 << 5);

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        CaptureFlags(self.0 | other.0)
    }

    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct CaptureRecord {
    pub seq: u64,
    pub instrument: Instrument,
    pub channel: Channel,
    pub ts_socket: Timestamp,
    pub ts_exchange: Option<Timestamp>,
    pub payload: Vec<u8>,
    pub flags: CaptureFlags,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct CaptureManifest {
    pub schema_version: u32,
    pub capture_id: CaptureId,
    pub created_at: Timestamp,
    pub instrument: Instrument,
    pub channel: Channel,
    pub frames_written: u64,
    pub stop_reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrument::{MarketType, Symbol, Venue};

    fn record() -> CaptureRecord {
        CaptureRecord {
            seq: 7,
            instrument: Instrument::new(
                Venue::Binance,
                MarketType::Spot,
                Symbol::new("BTC/USDT").unwrap(),
            ),
            channel: Channel::BookDiff,
            ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
            ts_exchange: Some(Timestamp::from_unix_nanos(1_699_999_999_999_000_000)),
            payload: vec![1, 2, 3],
            flags: CaptureFlags::SEQUENCE_GAP.union(CaptureFlags::UNRELIABLE),
        }
    }

    #[test]
    fn flags_combine_and_report() {
        let mut flags = CaptureFlags::SEQUENCE_GAP;
        flags.insert(CaptureFlags::STALE);
        assert!(flags.contains(CaptureFlags::SEQUENCE_GAP));
        assert!(flags.contains(CaptureFlags::STALE));
        assert!(!flags.contains(CaptureFlags::DUPLICATE));
        assert_eq!(flags.bits(), 0b1001);
    }

    #[test]
    fn records_round_trip_through_json() {
        let original = record();
        let json = serde_json::to_string(&original).unwrap();
        let decoded: CaptureRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn manifests_declare_the_schema_version() {
        let manifest = CaptureManifest {
            schema_version: SCHEMA_VERSION,
            capture_id: CaptureId::new("01HQ7Z0K9V8S4T2M"),
            created_at: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
            instrument: Instrument::new(
                Venue::Bybit,
                MarketType::PerpUsdt,
                Symbol::new("ETH/USDT").unwrap(),
            ),
            channel: Channel::Funding,
            frames_written: 0,
            stop_reason: Some("not_started".to_owned()),
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let decoded: CaptureManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded, manifest);
    }
}
