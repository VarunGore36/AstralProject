use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Binance,
    Bybit,
}

#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum VenueParseError {
    #[error("unknown venue: {0}")]
    Unknown(String),
}

impl Venue {
    pub const fn as_str(self) -> &'static str {
        match self {
            Venue::Binance => "binance",
            Venue::Bybit => "bybit",
        }
    }
}

impl FromStr for Venue {
    type Err = VenueParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "binance" => Ok(Venue::Binance),
            "bybit" => Ok(Venue::Bybit),
            other => Err(VenueParseError::Unknown(other.to_owned())),
        }
    }
}

impl fmt::Display for Venue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketType {
    Spot,
    PerpUsdt,
}

#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum MarketTypeParseError {
    #[error("unknown market type: {0}")]
    Unknown(String),
}

impl MarketType {
    pub const fn as_str(self) -> &'static str {
        match self {
            MarketType::Spot => "spot",
            MarketType::PerpUsdt => "perp_usdt",
        }
    }
}

impl FromStr for MarketType {
    type Err = MarketTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "spot" => Ok(MarketType::Spot),
            "perp_usdt" | "perp" => Ok(MarketType::PerpUsdt),
            other => Err(MarketTypeParseError::Unknown(other.to_owned())),
        }
    }
}

impl fmt::Display for MarketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Symbol(String);

#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum SymbolError {
    #[error("symbol must contain exactly one '/' separator: {0}")]
    Malformed(String),
    #[error("symbol base is empty: {0}")]
    EmptyBase(String),
    #[error("symbol quote is empty: {0}")]
    EmptyQuote(String),
    #[error(
        "symbol must be uppercase ASCII alphanumeric with '.', '_' or '-' inside each side: {0}"
    )]
    InvalidCharacters(String),
}

impl Symbol {
    pub fn new(value: impl Into<String>) -> Result<Self, SymbolError> {
        let value = value.into();
        let mut sides = value.split('/');
        let base = sides.next().unwrap_or_default();
        let quote = sides.next().unwrap_or_default();
        if sides.next().is_some() || !value.contains('/') {
            return Err(SymbolError::Malformed(value));
        }
        if base.is_empty() {
            return Err(SymbolError::EmptyBase(value));
        }
        if quote.is_empty() {
            return Err(SymbolError::EmptyQuote(value));
        }
        if !is_valid_side(base) || !is_valid_side(quote) {
            return Err(SymbolError::InvalidCharacters(value));
        }
        Ok(Symbol(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn base(&self) -> &str {
        self.0.split('/').next().unwrap_or_default()
    }

    pub fn quote(&self) -> &str {
        self.0.split('/').nth(1).unwrap_or_default()
    }
}

impl FromStr for Symbol {
    type Err = SymbolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Symbol::new(s)
    }
}

impl From<Symbol> for String {
    fn from(value: Symbol) -> Self {
        value.0
    }
}

impl TryFrom<String> for Symbol {
    type Error = SymbolError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Symbol::new(value)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn is_valid_side(side: &str) -> bool {
    side.bytes().all(|b| {
        b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'.' || b == b'_' || b == b'-'
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    BookDiff,
    BookSnapshot,
    Trade,
    BookTicker,
    Funding,
    OpenInterest,
    Liquidation,
}

#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum ChannelParseError {
    #[error("unknown channel: {0}")]
    Unknown(String),
}

impl Channel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Channel::BookDiff => "book_diff",
            Channel::BookSnapshot => "book_snapshot",
            Channel::Trade => "trade",
            Channel::BookTicker => "book_ticker",
            Channel::Funding => "funding",
            Channel::OpenInterest => "open_interest",
            Channel::Liquidation => "liquidation",
        }
    }
}

impl FromStr for Channel {
    type Err = ChannelParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "book_diff" | "book" => Ok(Channel::BookDiff),
            "book_snapshot" | "snapshot" => Ok(Channel::BookSnapshot),
            "trade" | "trades" => Ok(Channel::Trade),
            "book_ticker" | "ticker" => Ok(Channel::BookTicker),
            "funding" => Ok(Channel::Funding),
            "open_interest" | "oi" => Ok(Channel::OpenInterest),
            "liquidation" | "liquidations" => Ok(Channel::Liquidation),
            other => Err(ChannelParseError::Unknown(other.to_owned())),
        }
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct Instrument {
    venue: Venue,
    market_type: MarketType,
    symbol: Symbol,
}

impl Instrument {
    pub fn new(venue: Venue, market_type: MarketType, symbol: Symbol) -> Self {
        Instrument {
            venue,
            market_type,
            symbol,
        }
    }

    pub const fn venue(&self) -> Venue {
        self.venue
    }

    pub const fn market_type(&self) -> MarketType {
        self.market_type
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}

impl fmt::Display for Instrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.venue, self.market_type, self.symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbols_round_trip() {
        let symbol = Symbol::new("BTC/USDT").unwrap();
        assert_eq!(symbol.base(), "BTC");
        assert_eq!(symbol.quote(), "USDT");
        assert_eq!(symbol.to_string(), "BTC/USDT");
    }

    #[test]
    fn symbols_reject_malformed_input() {
        assert!(matches!(
            Symbol::new("BTCUSDT").unwrap_err(),
            SymbolError::Malformed(_)
        ));
        assert!(matches!(
            Symbol::new("BTC/USDT/USD").unwrap_err(),
            SymbolError::Malformed(_)
        ));
        assert!(matches!(
            Symbol::new("/USDT").unwrap_err(),
            SymbolError::EmptyBase(_)
        ));
        assert!(matches!(
            Symbol::new("btc/usdt").unwrap_err(),
            SymbolError::InvalidCharacters(_)
        ));
    }

    #[test]
    fn enums_parse_from_lowercase_names() {
        assert_eq!("binance".parse::<Venue>().unwrap(), Venue::Binance);
        assert_eq!("perp".parse::<MarketType>().unwrap(), MarketType::PerpUsdt);
        assert_eq!("book".parse::<Channel>().unwrap(), Channel::BookDiff);
        assert!("kraken".parse::<Venue>().is_err());
        assert!("candles".parse::<Channel>().is_err());
    }

    #[test]
    fn enums_serialize_canonically() {
        let instrument = Instrument::new(
            Venue::Bybit,
            MarketType::PerpUsdt,
            Symbol::new("ETH/USDT").unwrap(),
        );
        let json = serde_json::to_string(&instrument).unwrap();
        assert_eq!(
            json,
            r#"{"venue":"bybit","market_type":"perp_usdt","symbol":"ETH/USDT"}"#
        );
        assert_eq!(
            serde_json::from_str::<Instrument>(&json).unwrap(),
            instrument
        );
    }
}
