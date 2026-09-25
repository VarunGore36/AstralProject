use astra_types::{Channel, Instrument, MarketType, Venue};
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

fn binance_stream_url(instrument: &Instrument, channel: Channel) -> Result<String, FeedError> {
    let root = match instrument.market_type() {
        MarketType::Spot => BINANCE_SPOT_WS,
        MarketType::PerpUsdt => BINANCE_FUTURES_WS,
    };

    let symbol = stream_symbol(instrument)?;
    let stream = match channel {
        Channel::BookDiff => format!("{symbol}@depth@100ms"),
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
                Channel::Trade
            ),
            Err(FeedError::NotImplemented { .. })
        ));
    }
}
