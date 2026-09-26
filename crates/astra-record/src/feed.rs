use astra_book::{BookDiff, BookSnapshot, Level, UpdateSpan};
use astra_types::{Channel, Fixed, Instrument, MarketType, Venue};
use thiserror::Error;

const BINANCE_SPOT_WS: &str = "wss://stream.binance.com:9443/ws";
const BINANCE_FUTURES_WS: &str = "wss://fstream.binance.com/ws";

#[derive(Debug, Error)]
pub enum FeedError {
    #[error("no websocket feed is implemented for {venue} {market_type} {channel}")]
    NotImplemented {
        venue: String,
        market_type: String,
        channel: String,
    },
    #[error("symbol {symbol} has no stream name")]
    EmptySymbol { symbol: String },
}

pub fn stream_url(instrument: &Instrument, channel: Channel) -> Result<String, FeedError> {
    match instrument.venue() {
        Venue::Binance => binance_stream_url(instrument, channel),
        Venue::Bybit => Err(not_implemented(instrument, channel)),
    }
}

pub fn update_span(venue: Venue, channel: Channel, payload: &[u8]) -> Option<UpdateSpan> {
    match (venue, channel) {
        (Venue::Binance, Channel::BookDiff) => binance_depth_span(payload),
        _ => None,
    }
}

#[derive(serde::Deserialize)]
struct BinanceDepthEvent {
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    last_update_id: u64,
}

fn binance_depth_span(payload: &[u8]) -> Option<UpdateSpan> {
    let event: BinanceDepthEvent = serde_json::from_slice(payload).ok()?;
    Some(UpdateSpan::new(event.first_update_id, event.last_update_id))
}

pub fn book_snapshot(venue: Venue, channel: Channel, payload: &[u8]) -> Option<BookSnapshot> {
    match (venue, channel) {
        (Venue::Binance, Channel::BookSnapshot) => binance_book_snapshot(payload),
        _ => None,
    }
}

#[derive(serde::Deserialize)]
struct BinanceBookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "bids")]
    bids: Vec<(Fixed, Fixed)>,
    #[serde(rename = "asks")]
    asks: Vec<(Fixed, Fixed)>,
}

fn binance_book_snapshot(payload: &[u8]) -> Option<BookSnapshot> {
    let event: BinanceBookSnapshot = serde_json::from_slice(payload).ok()?;
    Some(BookSnapshot {
        last_update_id: event.last_update_id,
        bids: to_levels(event.bids),
        asks: to_levels(event.asks),
    })
}

pub fn book_diff(venue: Venue, channel: Channel, payload: &[u8]) -> Option<BookDiff> {
    match (venue, channel) {
        (Venue::Binance, Channel::BookDiff) => binance_depth_diff(payload),
        _ => None,
    }
}

#[derive(serde::Deserialize)]
struct BinanceDepthBook {
    #[serde(rename = "b")]
    bids: Vec<(Fixed, Fixed)>,
    #[serde(rename = "a")]
    asks: Vec<(Fixed, Fixed)>,
}

fn binance_depth_diff(payload: &[u8]) -> Option<BookDiff> {
    let event: BinanceDepthBook = serde_json::from_slice(payload).ok()?;
    Some(BookDiff {
        bids: to_levels(event.bids),
        asks: to_levels(event.asks),
    })
}

fn to_levels(levels: Vec<(Fixed, Fixed)>) -> Vec<Level> {
    levels
        .into_iter()
        .map(|(price, quantity)| Level { price, quantity })
        .collect()
}

fn binance_stream_url(instrument: &Instrument, channel: Channel) -> Result<String, FeedError> {
    let root = match instrument.market_type() {
        MarketType::Spot => BINANCE_SPOT_WS,
        MarketType::PerpUsdt => BINANCE_FUTURES_WS,
    };

    let symbol = stream_symbol(instrument)?;
    let stream = match (instrument.market_type(), channel) {
        (_, Channel::BookDiff) => format!("{symbol}@depth@100ms"),
        (_, Channel::BookSnapshot) => format!("{symbol}@depth10@100ms"),
        (_, Channel::Trade) => format!("{symbol}@trade"),
        (_, Channel::BookTicker) => format!("{symbol}@bookTicker"),
        (MarketType::PerpUsdt, Channel::Funding) => format!("{symbol}@markPrice@1s"),
        (MarketType::PerpUsdt, Channel::Liquidation) => format!("{symbol}@forceOrder"),
        _ => return Err(not_implemented(instrument, channel)),
    };

    Ok(format!("{root}/{stream}"))
}

fn stream_symbol(instrument: &Instrument) -> Result<String, FeedError> {
    let symbol: String = instrument
        .symbol()
        .as_str()
        .chars()
        .filter(|character| *character != '/')
        .collect::<String>()
        .to_ascii_lowercase();

    if symbol.is_empty() {
        return Err(FeedError::EmptySymbol {
            symbol: instrument.symbol().to_string(),
        });
    }

    Ok(symbol)
}

fn not_implemented(instrument: &Instrument, channel: Channel) -> FeedError {
    FeedError::NotImplemented {
        venue: instrument.venue().to_string(),
        market_type: instrument.market_type().to_string(),
        channel: channel.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use astra_types::Symbol;

    fn instrument(venue: Venue, market_type: MarketType) -> Instrument {
        Instrument::new(venue, market_type, Symbol::new("BTC/USDT").unwrap())
    }

    #[test]
    fn binance_spot_depth_maps_to_the_diff_stream() {
        let url = stream_url(
            &instrument(Venue::Binance, MarketType::Spot),
            Channel::BookDiff,
        )
        .unwrap();
        assert_eq!(url, "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms");
    }

    #[test]
    fn binance_perp_depth_maps_to_the_futures_diff_stream() {
        let url = stream_url(
            &instrument(Venue::Binance, MarketType::PerpUsdt),
            Channel::BookDiff,
        )
        .unwrap();
        assert_eq!(url, "wss://fstream.binance.com/ws/btcusdt@depth@100ms");
    }

    #[test]
    fn unimplemented_combinations_are_refused() {
        assert!(matches!(
            stream_url(
                &instrument(Venue::Bybit, MarketType::Spot),
                Channel::BookDiff
            ),
            Err(FeedError::NotImplemented { .. })
        ));
        assert!(matches!(
            stream_url(
                &instrument(Venue::Binance, MarketType::Spot),
                Channel::Funding
            ),
            Err(FeedError::NotImplemented { .. })
        ));
        assert!(matches!(
            stream_url(
                &instrument(Venue::Binance, MarketType::Spot),
                Channel::Liquidation
            ),
            Err(FeedError::NotImplemented { .. })
        ));
    }

    #[test]
    fn every_supported_channel_maps_to_its_venue_stream() {
        let spot = instrument(Venue::Binance, MarketType::Spot);
        assert_eq!(
            stream_url(&spot, Channel::Trade).unwrap(),
            "wss://stream.binance.com:9443/ws/btcusdt@trade"
        );
        assert_eq!(
            stream_url(&spot, Channel::BookTicker).unwrap(),
            "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
        );
        assert_eq!(
            stream_url(&spot, Channel::BookSnapshot).unwrap(),
            "wss://stream.binance.com:9443/ws/btcusdt@depth10@100ms"
        );

        let perp = instrument(Venue::Binance, MarketType::PerpUsdt);
        assert_eq!(
            stream_url(&perp, Channel::Funding).unwrap(),
            "wss://fstream.binance.com/ws/btcusdt@markPrice@1s"
        );
        assert_eq!(
            stream_url(&perp, Channel::Liquidation).unwrap(),
            "wss://fstream.binance.com/ws/btcusdt@forceOrder"
        );
    }

    #[test]
    fn open_interest_is_not_a_native_websocket_stream() {
        let perp = instrument(Venue::Binance, MarketType::PerpUsdt);
        assert!(matches!(
            stream_url(&perp, Channel::OpenInterest),
            Err(FeedError::NotImplemented { .. })
        ));
    }

    #[test]
    fn a_real_captured_frame_yields_its_update_span() {
        let payload = include_str!("../testdata/binance_depth_update.json");
        let span = update_span(Venue::Binance, Channel::BookDiff, payload.as_bytes()).unwrap();
        assert_eq!(span.first, 100697441890);
        assert_eq!(span.last, 100697441922);
    }

    #[test]
    fn payloads_without_update_ids_are_not_checked() {
        assert!(update_span(Venue::Binance, Channel::BookDiff, b"{\"e\":\"trade\"}").is_none());
        assert!(update_span(Venue::Binance, Channel::BookDiff, b"not json").is_none());
        assert!(update_span(Venue::Binance, Channel::BookDiff, b"").is_none());
        assert!(update_span(Venue::Binance, Channel::Trade, b"{\"U\":1,\"u\":2}").is_none());
    }
}
