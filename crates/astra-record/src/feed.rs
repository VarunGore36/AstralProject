use astra_book::{BookDiff, BookSnapshot, Level, UpdateSpan};
use astra_types::{Channel, Fixed, Instrument, MarketType, Timestamp, Venue};
use thiserror::Error;

const BINANCE_SPOT_WS: &str = "wss://stream.binance.com:9443/ws";
const BINANCE_FUTURES_WS: &str = "wss://fstream.binance.com/ws";
const BYBIT_SPOT_WS: &str = "wss://stream.bybit.com/v5/public/spot";
const BYBIT_LINEAR_WS: &str = "wss://stream.bybit.com/v5/public/linear";
const COINBASE_WS: &str = "wss://ws-feed.exchange.coinbase.com";

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
        Venue::Bybit => bybit_stream_url(instrument, channel),
        Venue::Coinbase => coinbase_stream_url(instrument, channel),
    }
}

pub fn subscribe_message(instrument: &Instrument, channel: Channel) -> Option<String> {
    match (instrument.venue(), channel) {
        (Venue::Bybit, Channel::BookDiff) => Some(bybit_subscribe("orderbook.50", instrument)),
        (Venue::Bybit, Channel::Trade) => Some(bybit_subscribe("publicTrade", instrument)),
        (Venue::Bybit, Channel::Liquidation)
            if instrument.market_type() == MarketType::PerpUsdt =>
        {
            Some(bybit_subscribe("allLiquidation", instrument))
        }
        (Venue::Coinbase, Channel::Trade) => Some(coinbase_subscribe("matches", instrument)),
        (Venue::Coinbase, Channel::BookTicker) => Some(coinbase_subscribe("ticker", instrument)),
        _ => None,
    }
}

fn bybit_subscribe(topic_prefix: &str, instrument: &Instrument) -> String {
    let topic = format!("{topic_prefix}.{}", bybit_symbol(instrument));
    format!("{{\"op\":\"subscribe\",\"args\":[\"{topic}\"]}}")
}

fn coinbase_subscribe(channel: &str, instrument: &Instrument) -> String {
    let symbol = coinbase_symbol(instrument);
    format!(
        "{{\"type\":\"subscribe\",\"channels\":[{{\"name\":\"{channel}\",\"product_ids\":[\"{symbol}\"]}}]}}"
    )
}

fn coinbase_symbol(instrument: &Instrument) -> String {
    instrument.symbol().as_str().replace('/', "-")
}

fn bybit_symbol(instrument: &Instrument) -> String {
    instrument
        .symbol()
        .as_str()
        .chars()
        .filter(|character| *character != '/')
        .collect::<String>()
        .to_ascii_uppercase()
}

pub fn update_span(venue: Venue, channel: Channel, payload: &[u8]) -> Option<UpdateSpan> {
    match (venue, channel) {
        (Venue::Binance, Channel::BookDiff) => binance_depth_span(payload),
        (Venue::Bybit, Channel::BookDiff) => bybit_orderbook_span(payload),
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

#[derive(serde::Deserialize)]
struct BybitOrderbookData {
    #[serde(rename = "b")]
    bids: Vec<(Fixed, Fixed)>,
    #[serde(rename = "a")]
    asks: Vec<(Fixed, Fixed)>,
    #[serde(rename = "u")]
    version: u64,
}

#[derive(serde::Deserialize)]
struct BybitOrderbook {
    #[serde(rename = "type", default)]
    kind: String,
    data: BybitOrderbookData,
}

pub fn inband_snapshot(venue: Venue, channel: Channel, payload: &[u8]) -> Option<BookSnapshot> {
    match (venue, channel) {
        (Venue::Bybit, Channel::BookDiff) => bybit_inband_snapshot(payload),
        _ => None,
    }
}

fn bybit_inband_snapshot(payload: &[u8]) -> Option<BookSnapshot> {
    let event: BybitOrderbook = serde_json::from_slice(payload).ok()?;
    if event.kind != "snapshot" {
        return None;
    }
    Some(BookSnapshot {
        last_update_id: event.data.version,
        bids: to_levels(event.data.bids),
        asks: to_levels(event.data.asks),
    })
}

fn bybit_orderbook_span(payload: &[u8]) -> Option<UpdateSpan> {
    let event: BybitOrderbook = serde_json::from_slice(payload).ok()?;
    Some(UpdateSpan::new(event.data.version, event.data.version))
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
        (Venue::Bybit, Channel::BookDiff) => bybit_orderbook_diff(payload),
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

fn bybit_orderbook_diff(payload: &[u8]) -> Option<BookDiff> {
    let event: BybitOrderbook = serde_json::from_slice(payload).ok()?;
    Some(BookDiff {
        bids: to_levels(event.data.bids),
        asks: to_levels(event.data.asks),
    })
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TradePrint {
    pub trade_id: Option<String>,
    pub price: Fixed,
    pub quantity: Fixed,
    pub side: Option<String>,
    pub ts_exchange: Option<Timestamp>,
}

pub fn trade_prints(venue: Venue, channel: Channel, payload: &[u8]) -> Option<Vec<TradePrint>> {
    match (venue, channel) {
        (Venue::Binance, Channel::Trade) => binance_trade(payload).map(|print| vec![print]),
        (Venue::Bybit, Channel::Trade) => bybit_trades(payload),
        (Venue::Coinbase, Channel::Trade) => coinbase_trade(payload).map(|print| vec![print]),
        _ => None,
    }
}

#[derive(serde::Deserialize)]
struct BinanceTrade {
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p")]
    price: Fixed,
    #[serde(rename = "q")]
    quantity: Fixed,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
    #[serde(rename = "T")]
    trade_time_millis: i64,
}

fn binance_trade(payload: &[u8]) -> Option<TradePrint> {
    let event: BinanceTrade = serde_json::from_slice(payload).ok()?;
    Some(TradePrint {
        trade_id: Some(event.trade_id.to_string()),
        price: event.price,
        quantity: event.quantity,
        side: Some(if event.buyer_is_maker { "Sell" } else { "Buy" }.to_owned()),
        ts_exchange: Some(Timestamp::from_unix_nanos(
            event.trade_time_millis.saturating_mul(1_000_000),
        )),
    })
}

#[derive(serde::Deserialize)]
struct BybitTrade {
    #[serde(rename = "T")]
    trade_time_millis: i64,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "v")]
    quantity: Fixed,
    #[serde(rename = "p")]
    price: Fixed,
    #[serde(rename = "i")]
    trade_id: String,
}

#[derive(serde::Deserialize)]
struct BybitTradeStream {
    data: Vec<BybitTrade>,
}

fn bybit_trades(payload: &[u8]) -> Option<Vec<TradePrint>> {
    let event: BybitTradeStream = serde_json::from_slice(payload).ok()?;
    Some(
        event
            .data
            .into_iter()
            .map(|trade| TradePrint {
                trade_id: Some(trade.trade_id),
                price: trade.price,
                quantity: trade.quantity,
                side: Some(trade.side),
                ts_exchange: Some(Timestamp::from_unix_nanos(
                    trade.trade_time_millis.saturating_mul(1_000_000),
                )),
            })
            .collect(),
    )
}

#[derive(serde::Deserialize)]
struct CoinbaseMatch {
    trade_id: u64,
    price: Fixed,
    size: Fixed,
    side: String,
    time: String,
}

fn coinbase_trade(payload: &[u8]) -> Option<TradePrint> {
    let event: CoinbaseMatch = serde_json::from_slice(payload).ok()?;
    Some(TradePrint {
        trade_id: Some(event.trade_id.to_string()),
        price: event.price,
        quantity: event.size,
        side: Some(capitalize(&event.side)),
        ts_exchange: parse_rfc3339_nanos(&event.time),
    })
}

fn capitalize(side: &str) -> String {
    let mut chars = side.chars();
    match chars.next() {
        Some(first) => {
            let mut result: String = first.to_uppercase().collect();
            result.push_str(&chars.as_str().to_lowercase());
            result
        }
        None => String::new(),
    }
}

fn parse_rfc3339_nanos(time: &str) -> Option<Timestamp> {
    use chrono::DateTime;
    let parsed: DateTime<chrono::Utc> = time.parse().ok()?;
    Some(Timestamp::from_unix_nanos(parsed.timestamp_nanos_opt()?))
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

fn bybit_stream_url(instrument: &Instrument, channel: Channel) -> Result<String, FeedError> {
    let root = match instrument.market_type() {
        MarketType::Spot => BYBIT_SPOT_WS,
        MarketType::PerpUsdt => BYBIT_LINEAR_WS,
    };

    match channel {
        Channel::BookDiff | Channel::Trade => Ok(root.to_owned()),
        Channel::Liquidation if instrument.market_type() == MarketType::PerpUsdt => {
            Ok(root.to_owned())
        }
        _ => Err(not_implemented(instrument, channel)),
    }
}

fn coinbase_stream_url(instrument: &Instrument, channel: Channel) -> Result<String, FeedError> {
    match (instrument.market_type(), channel) {
        (MarketType::Spot, Channel::Trade | Channel::BookTicker) => Ok(COINBASE_WS.to_owned()),
        _ => Err(not_implemented(instrument, channel)),
    }
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
                Channel::Funding
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
    fn bybit_book_diff_maps_to_base_urls() {
        assert_eq!(
            stream_url(
                &instrument(Venue::Bybit, MarketType::Spot),
                Channel::BookDiff
            )
            .unwrap(),
            "wss://stream.bybit.com/v5/public/spot"
        );
        assert_eq!(
            stream_url(
                &instrument(Venue::Bybit, MarketType::PerpUsdt),
                Channel::BookDiff
            )
            .unwrap(),
            "wss://stream.bybit.com/v5/public/linear"
        );
    }

    #[test]
    fn bybit_trade_and_liquidation_map_to_documented_topics() {
        let spot = instrument(Venue::Bybit, MarketType::Spot);
        assert_eq!(
            stream_url(&spot, Channel::Trade).unwrap(),
            "wss://stream.bybit.com/v5/public/spot"
        );
        assert_eq!(
            subscribe_message(&spot, Channel::Trade).unwrap(),
            "{\"op\":\"subscribe\",\"args\":[\"publicTrade.BTCUSDT\"]}"
        );

        let perp = instrument(Venue::Bybit, MarketType::PerpUsdt);
        assert_eq!(
            subscribe_message(&perp, Channel::Liquidation).unwrap(),
            "{\"op\":\"subscribe\",\"args\":[\"allLiquidation.BTCUSDT\"]}"
        );
    }

    #[test]
    fn bybit_refuses_channels_without_native_streams() {
        let spot = instrument(Venue::Bybit, MarketType::Spot);
        for channel in [
            Channel::Liquidation,
            Channel::Funding,
            Channel::BookTicker,
            Channel::BookSnapshot,
            Channel::OpenInterest,
        ] {
            assert!(
                matches!(
                    stream_url(&spot, channel),
                    Err(FeedError::NotImplemented { .. })
                ),
                "{channel} should be refused on Bybit spot"
            );
            assert_eq!(subscribe_message(&spot, channel), None);
        }
    }

    #[test]
    fn bybit_subscribes_to_orderbook_50_after_connect() {
        assert_eq!(
            subscribe_message(
                &instrument(Venue::Bybit, MarketType::Spot),
                Channel::BookDiff
            )
            .unwrap(),
            "{\"op\":\"subscribe\",\"args\":[\"orderbook.50.BTCUSDT\"]}"
        );
    }

    #[test]
    fn binance_needs_no_subscribe_message() {
        assert_eq!(
            subscribe_message(
                &instrument(Venue::Binance, MarketType::Spot),
                Channel::BookDiff
            ),
            None
        );
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
    fn coinbase_trade_and_ticker_map_to_the_shared_socket() {
        let coinbase = Instrument::new(
            Venue::Coinbase,
            MarketType::Spot,
            Symbol::new("BTC/USD").unwrap(),
        );
        assert_eq!(
            stream_url(&coinbase, Channel::Trade).unwrap(),
            "wss://ws-feed.exchange.coinbase.com"
        );
        assert_eq!(
            stream_url(&coinbase, Channel::BookTicker).unwrap(),
            "wss://ws-feed.exchange.coinbase.com"
        );
        assert_eq!(
            subscribe_message(&coinbase, Channel::Trade).unwrap(),
            "{\"type\":\"subscribe\",\"channels\":[{\"name\":\"matches\",\"product_ids\":[\"BTC-USD\"]}]}"
        );
        assert_eq!(
            subscribe_message(&coinbase, Channel::BookTicker).unwrap(),
            "{\"type\":\"subscribe\",\"channels\":[{\"name\":\"ticker\",\"product_ids\":[\"BTC-USD\"]}]}"
        );
    }

    #[test]
    fn coinbase_book_diff_is_refused_for_lack_of_a_public_stream() {
        let coinbase = Instrument::new(
            Venue::Coinbase,
            MarketType::Spot,
            Symbol::new("BTC/USD").unwrap(),
        );
        assert!(matches!(
            stream_url(&coinbase, Channel::BookDiff),
            Err(FeedError::NotImplemented { .. })
        ));
        assert_eq!(subscribe_message(&coinbase, Channel::BookDiff), None);
        assert!(matches!(
            stream_url(&coinbase, Channel::Funding),
            Err(FeedError::NotImplemented { .. })
        ));
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
    fn eight_consecutive_real_frames_are_continuous() {
        let payload = include_str!("../testdata/binance_depth_sequence.json");
        let frames: Vec<serde_json::Value> = serde_json::from_str(payload).unwrap();
        assert_eq!(frames.len(), 8);

        let spans: Vec<UpdateSpan> = frames
            .iter()
            .map(|frame| {
                let bytes = serde_json::to_vec(frame).unwrap();
                update_span(Venue::Binance, Channel::BookDiff, &bytes).unwrap()
            })
            .collect();

        for (previous, next) in spans.iter().zip(spans.iter().skip(1)) {
            assert_eq!(next.first, previous.last + 1);
        }
    }

    #[test]
    fn hostile_bytes_never_panic_and_never_parse() {
        let hostile: &[&[u8]] = &[
            b"",
            b"not json",
            b"{",
            b"{\"U\":1",
            b"{\"U\":\"abc\",\"u\":1}",
            b"{\"U\":1,\"u\":\"abc\"}",
            b"{\"U\":-1,\"u\":1}",
            b"{\"U\":18446744073709551616,\"u\":1}",
            b"{\"U\":1,\"u\":18446744073709551616}",
            b"{\"U\":1.5,\"u\":2}",
            b"{\"U\":null,\"u\":null}",
            b"{\"U\":[1],\"u\":{\"n\":2}}",
            b"[1,2,3]",
            b"42",
            b"\"U\"",
            &[0xFF, 0xFE, 0x00, 0x80],
            &[0x00, 0x01, 0x02],
            b"\xef\xbb\xbf{\"U\":1,\"u\":2}",
        ];

        for payload in hostile {
            assert_eq!(
                update_span(Venue::Binance, Channel::BookDiff, payload),
                None,
                "payload parsed that should not have: {payload:?}"
            );
            assert_eq!(
                book_diff(Venue::Binance, Channel::BookDiff, payload),
                None,
                "payload parsed that should not have: {payload:?}"
            );
            assert_eq!(
                book_snapshot(Venue::Binance, Channel::BookSnapshot, payload),
                None,
                "payload parsed that should not have: {payload:?}"
            );
        }
    }

    #[test]
    fn hostile_book_payloads_never_panic_and_never_parse() {
        let hostile: &[&[u8]] = &[
            b"{\"b\":[[\"1.00000000\"]],\"a\":[]}",
            b"{\"b\":[[\"abc\",\"1\"]],\"a\":[]}",
            b"{\"b\":\"not a list\",\"a\":[]}",
            b"{\"b\":[],\"a\":null}",
            b"{\"lastUpdateId\":\"abc\",\"bids\":[],\"asks\":[]}",
            b"{\"lastUpdateId\":-5,\"bids\":[],\"asks\":[]}",
            b"{\"bids\":[[\"1.00000000\",\"1\"]]}",
        ];

        for payload in hostile {
            assert_eq!(
                book_diff(Venue::Binance, Channel::BookDiff, payload),
                None,
                "payload parsed that should not have: {payload:?}"
            );
            assert_eq!(
                book_snapshot(Venue::Binance, Channel::BookSnapshot, payload),
                None,
                "payload parsed that should not have: {payload:?}"
            );
        }
    }

    #[test]
    fn a_negative_quantity_parses_but_is_refused_at_apply_time() {
        let payload = b"{\"b\":[[\"1.00000000\",\"-1\"]],\"a\":[]}";
        let diff = book_diff(Venue::Binance, Channel::BookDiff, payload).unwrap();

        let mut book = astra_book::OrderBook::new();
        assert!(matches!(
            book.apply_diff(&diff),
            Err(astra_book::BookError::InvalidQuantity(_))
        ));
    }

    #[test]
    fn payloads_without_update_ids_are_not_checked() {
        assert!(update_span(Venue::Binance, Channel::BookDiff, b"{\"e\":\"trade\"}").is_none());
        assert!(update_span(Venue::Binance, Channel::BookDiff, b"not json").is_none());
        assert!(update_span(Venue::Binance, Channel::BookDiff, b"").is_none());
        assert!(update_span(Venue::Binance, Channel::Trade, b"{\"U\":1,\"u\":2}").is_none());
    }

    #[test]
    fn four_real_bybit_frames_are_continuous() {
        let payload = include_str!("../testdata/bybit_snapshot_deltas.json");
        let frames: Vec<serde_json::Value> = serde_json::from_str(payload).unwrap();
        assert_eq!(frames.len(), 4);

        let spans: Vec<UpdateSpan> = frames
            .iter()
            .map(|frame| {
                let bytes = serde_json::to_vec(frame).unwrap();
                update_span(Venue::Bybit, Channel::BookDiff, &bytes).unwrap()
            })
            .collect();

        assert_eq!(spans[0], UpdateSpan::new(298329277, 298329277));
        for (previous, next) in spans.iter().zip(spans.iter().skip(1)) {
            assert_eq!(next.first, previous.last + 1);
        }
    }

    #[test]
    fn a_real_bybit_snapshot_yields_fifty_levels_a_side() {
        let payload = include_str!("../testdata/bybit_snapshot_deltas.json");
        let frames: Vec<serde_json::Value> = serde_json::from_str(payload).unwrap();
        let bytes = serde_json::to_vec(&frames[0]).unwrap();

        let diff = book_diff(Venue::Bybit, Channel::BookDiff, &bytes).unwrap();
        assert_eq!(diff.bids.len(), 50);
        assert_eq!(diff.asks.len(), 50);
        assert_eq!(diff.bids[0].price.to_string(), "82994.80000000");
        assert_eq!(diff.asks[0].price.to_string(), "82994.90000000");
    }

    #[test]
    fn a_real_bybit_delta_yields_only_its_changes() {
        let payload = include_str!("../testdata/bybit_snapshot_deltas.json");
        let frames: Vec<serde_json::Value> = serde_json::from_str(payload).unwrap();
        let bytes = serde_json::to_vec(&frames[1]).unwrap();

        let diff = book_diff(Venue::Bybit, Channel::BookDiff, &bytes).unwrap();
        assert_eq!(diff.bids.len(), 0);
        assert_eq!(diff.asks.len(), 2);
    }

    #[test]
    fn bybit_subscribe_confirmations_are_not_books() {
        let payload = b"{\"success\":true,\"ret_msg\":\"subscribe\",\"conn_id\":\"abc\",\"op\":\"subscribe\"}";
        assert_eq!(update_span(Venue::Bybit, Channel::BookDiff, payload), None);
        assert_eq!(book_diff(Venue::Bybit, Channel::BookDiff, payload), None);
    }

    #[test]
    fn only_snapshot_frames_are_inband_snapshots() {
        let payload = include_str!("../testdata/bybit_snapshot_deltas.json");
        let frames: Vec<serde_json::Value> = serde_json::from_str(payload).unwrap();

        let snapshot_bytes = serde_json::to_vec(&frames[0]).unwrap();
        let snapshot = inband_snapshot(Venue::Bybit, Channel::BookDiff, &snapshot_bytes).unwrap();
        assert_eq!(snapshot.last_update_id, 298329277);
        assert_eq!(snapshot.bids.len(), 50);
        assert_eq!(snapshot.asks.len(), 50);

        for frame in frames.iter().skip(1) {
            let bytes = serde_json::to_vec(frame).unwrap();
            assert_eq!(
                inband_snapshot(Venue::Bybit, Channel::BookDiff, &bytes),
                None
            );
        }

        assert_eq!(
            inband_snapshot(
                Venue::Bybit,
                Channel::BookDiff,
                b"{\"success\":true,\"op\":\"subscribe\"}"
            ),
            None
        );
        assert_eq!(
            inband_snapshot(Venue::Binance, Channel::BookDiff, &snapshot_bytes),
            None
        );
    }

    #[test]
    fn a_real_binance_trade_parses_exactly() {
        let payload = include_str!("../testdata/binance_trade.json");
        let prints = trade_prints(Venue::Binance, Channel::Trade, payload.as_bytes()).unwrap();

        assert_eq!(prints.len(), 1);
        assert_eq!(prints[0].trade_id.as_deref(), Some("6736601518"));
        assert_eq!(prints[0].price.to_string(), "85976.95000000");
        assert_eq!(prints[0].quantity.to_string(), "0.00148000");
        assert_eq!(prints[0].side.as_deref(), Some("Buy"));
        assert_eq!(
            prints[0].ts_exchange.map(|ts| ts.unix_nanos()),
            Some(1_791_193_512_031_000_000)
        );
    }

    #[test]
    fn a_documented_bybit_trade_parses_exactly() {
        let payload = include_str!("../testdata/bybit_trade_doc_example.json");
        let prints = trade_prints(Venue::Bybit, Channel::Trade, payload.as_bytes()).unwrap();

        assert_eq!(prints.len(), 1);
        assert_eq!(
            prints[0].trade_id.as_deref(),
            Some("20f43950-d8dd-5b31-9112-a178eb6023af")
        );
        assert_eq!(prints[0].price.to_string(), "16578.50000000");
        assert_eq!(prints[0].side.as_deref(), Some("Buy"));
    }

    #[test]
    fn a_bundled_bybit_message_expands_to_one_row_per_trade() {
        let payload = br#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672304486868,"data":[{"T":1672304486865,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"16578.50","i":"aaa","seq":1},{"T":1672304486866,"s":"BTCUSDT","S":"Sell","v":"0.002","p":"16578.51","i":"bbb","seq":2}]}"#;
        let prints = trade_prints(Venue::Bybit, Channel::Trade, payload).unwrap();

        assert_eq!(prints.len(), 2);
        assert_eq!(prints[0].trade_id.as_deref(), Some("aaa"));
        assert_eq!(prints[1].trade_id.as_deref(), Some("bbb"));
        assert_eq!(prints[1].side.as_deref(), Some("Sell"));
    }

    #[test]
    fn a_real_coinbase_match_parses_with_normalized_side() {
        let payload = include_str!("../testdata/coinbase_match_ticker.json");
        let frames: Vec<serde_json::Value> = serde_json::from_str(payload).unwrap();
        let trade = frames
            .iter()
            .find(|frame| frame.get("type") == Some(&serde_json::json!("match")))
            .unwrap();
        let bytes = serde_json::to_vec(trade).unwrap();
        let prints = trade_prints(Venue::Coinbase, Channel::Trade, &bytes).unwrap();

        assert_eq!(prints.len(), 1);
        assert_eq!(prints[0].trade_id.as_deref(), Some("1100092822"));
        assert_eq!(prints[0].price.to_string(), "83098.17000000");
        assert_eq!(prints[0].side.as_deref(), Some("Buy"));
        assert!(prints[0].ts_exchange.is_some());
    }

    #[test]
    fn hostile_trade_payloads_never_panic_and_never_parse() {
        let hostile: &[&[u8]] = &[
            b"",
            b"not json",
            b"{}",
            b"{\"e\":\"trade\"}",
            b"{\"t\":\"abc\",\"p\":\"1\",\"q\":\"1\",\"m\":true,\"T\":1}",
            b"{\"t\":1,\"p\":\"abc\",\"q\":\"1\",\"m\":true,\"T\":1}",
            b"{\"data\":\"not a list\"}",
            b"{\"data\":[{\"T\":1}]}",
        ];

        for payload in hostile {
            assert_eq!(
                trade_prints(Venue::Binance, Channel::Trade, payload),
                None,
                "payload parsed that should not have: {payload:?}"
            );
            assert_eq!(
                trade_prints(Venue::Bybit, Channel::Trade, payload),
                None,
                "payload parsed that should not have: {payload:?}"
            );
            assert_eq!(
                trade_prints(Venue::Coinbase, Channel::Trade, payload),
                None,
                "payload parsed that should not have: {payload:?}"
            );
        }
    }

    #[test]
    fn an_unparseable_coinbase_time_nulls_the_timestamp_only() {
        let payload = br#"{"trade_id":7,"price":"1.00000000","size":"0.5","side":"sell","time":"not a time"}"#;
        let prints = trade_prints(Venue::Coinbase, Channel::Trade, payload).unwrap();

        assert_eq!(prints.len(), 1);
        assert_eq!(prints[0].side.as_deref(), Some("Sell"));
        assert_eq!(prints[0].ts_exchange, None);
    }

    #[test]
    fn trade_parsing_refuses_other_channels() {
        let payload = include_str!("../testdata/binance_trade.json");
        assert_eq!(
            trade_prints(Venue::Binance, Channel::BookDiff, payload.as_bytes()),
            None
        );
    }
}
