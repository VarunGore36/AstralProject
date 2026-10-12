//! Replay over normalized Parquet instead of raw captures.
//!
//! This is the unification the failure policy waited for: an unparseable
//! venue frame normalizes to a null-field row, and raw replay counts the
//! same frame as `skipped_unparseable` (no event). The one answer is that a
//! null row replays as a skip — never as an invented event — so raw replay
//! and normalized replay of the same capture emit identical signals.
//!
//! Scale note, stated plainly: this reader collects event descriptors and
//! sorts them by `(seq, print_index)` before feeding the strategy, so its
//! memory is O(events). The soak-scale path is raw chunk-streamed replay
//! ([`crate::replay::replay`]); this module is the equivalence prover and
//! the research-slice reader, not the multi-GB engine. Snapshots are not
//! normalized (see the failure policy), so snapshot events never appear
//! here — exactly as if the raw stream held none.

use std::path::{Path, PathBuf};

use arrow::array::{
    Array, BooleanArray, Decimal128Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, UInt32Array, UInt64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use astra_book::{BookDiff, Level, UpdateSpan};
use astra_record::feed::{TopOfBook, TradePrint};
use astra_types::{Fixed, GapMarker, Timestamp};

use crate::replay::{
    BookDiffEvent, Context, ReplayError, ReplayReport, Strategy, TopBookEvent, TradeEvent,
    signal_hash,
};

/// Value the normalizer writes for its schema; anything else is refused loud.
const NORMALIZED_SCHEMA_VERSION: &str = "1";
const SCHEMA_VERSION_KEY: &str = "astra.schema_version";

/// One side of a normalized book level list: (price, quantity) raw pairs.
type BookLevels = Vec<(i128, i128)>;

/// One row read back from Parquet, before the null-row rule is applied.
enum NormEvent {
    Diff {
        seq: u64,
        ts_socket: Timestamp,
        ts_exchange: Option<Timestamp>,
        span: Option<(u64, u64)>,
        levels: Option<(BookLevels, BookLevels)>,
    },
    Trade {
        seq: u64,
        print_index: u32,
        ts_socket: Timestamp,
        ts_exchange: Option<Timestamp>,
        trade_id: Option<String>,
        price: Option<i128>,
        quantity: Option<i128>,
        side: Option<String>,
    },
    Top {
        seq: u64,
        ts_socket: Timestamp,
        ts_exchange: Option<Timestamp>,
        bid: Option<(i128, i128)>,
        ask: Option<(i128, i128)>,
    },
    Gap {
        seq: u64,
        ts_socket: Timestamp,
        marker: GapMarker,
    },
}

/// Replay a normalized output tree through a strategy.
///
/// `output` is the directory `normalize` wrote (Hive-partitioned Parquet).
/// Table identity comes from the `channel=` path component; a parquet file
/// without one is refused rather than guessed at.
pub fn replay_normalized(
    output: &Path,
    seed: u64,
    strategy: &mut dyn Strategy,
) -> Result<ReplayReport, ReplayError> {
    let mut files = parquet_files(output);
    if files.is_empty() {
        return Err(ReplayError::Malformed(format!(
            "no parquet files under {}",
            output.display()
        )));
    }
    files.sort();

    let mut events = Vec::new();
    for path in &files {
        read_table_events(path, &mut events)?;
    }
    // One record normalizes into exactly one table, and duplicate keys are
    // rejected at normalize time, so (seq, print_index) orders the stream
    // exactly as the raw record order did.
    events.sort_by_key(|event| (event_seq(event), event_sub(event)));

    let mut ctx = Context::seeded(seed);
    let mut report = ReplayReport {
        frames: events.len() as u64,
        ..ReplayReport::default()
    };

    for event in &events {
        feed_event(event, strategy, &mut ctx, &mut report)?;
    }

    strategy.on_end(report.frames, &mut ctx);
    report.signals = ctx.signals().len() as u64;
    report.signal_hash = signal_hash(ctx.signals());

    Ok(report)
}

fn event_seq(event: &NormEvent) -> u64 {
    match event {
        NormEvent::Diff { seq, .. }
        | NormEvent::Trade { seq, .. }
        | NormEvent::Top { seq, .. }
        | NormEvent::Gap { seq, .. } => *seq,
    }
}

fn event_sub(event: &NormEvent) -> u32 {
    match event {
        NormEvent::Trade { print_index, .. } => *print_index,
        NormEvent::Diff { .. } | NormEvent::Top { .. } | NormEvent::Gap { .. } => 0,
    }
}

/// The null-row rule, mirroring raw replay exactly: a row with content
/// emits; a null row counts as `skipped_unparseable`; a synthetic row
/// surfaces its gap. Raw replay emits a trade/topbook/diff event if and
/// only if the parser produced content, and every parser that succeeds
/// fills every content column — so "any content missing" is precisely "raw
/// saw nothing".
fn feed_event(
    event: &NormEvent,
    strategy: &mut dyn Strategy,
    ctx: &mut Context,
    report: &mut ReplayReport,
) -> Result<(), ReplayError> {
    match event {
        NormEvent::Gap {
            ts_socket, marker, ..
        } => {
            ctx.set_now(*ts_socket);
            strategy.on_gap(marker, ctx);
            report.gaps += 1;
            report.events_emitted += 1;
        }
        NormEvent::Diff {
            seq,
            ts_socket,
            ts_exchange,
            span,
            levels,
        } => {
            ctx.set_now(*ts_socket);
            let (Some((first, last)), Some((bids, asks))) = (span, levels) else {
                report.skipped_unparseable += 1;
                return Ok(());
            };
            strategy.on_book_diff(
                &BookDiffEvent {
                    seq: *seq,
                    ts_socket: *ts_socket,
                    ts_exchange: *ts_exchange,
                    span: UpdateSpan::new(*first, *last),
                    diff: BookDiff {
                        bids: to_levels(bids),
                        asks: to_levels(asks),
                    },
                },
                ctx,
            );
            report.events_emitted += 1;
        }
        NormEvent::Trade {
            seq,
            print_index,
            ts_socket,
            ts_exchange,
            trade_id,
            price,
            quantity,
            side,
        } => {
            ctx.set_now(*ts_socket);
            let (Some(price), Some(quantity)) = (price, quantity) else {
                report.skipped_unparseable += 1;
                return Ok(());
            };
            strategy.on_trade(
                &TradeEvent {
                    seq: *seq,
                    print_index: *print_index,
                    ts_socket: *ts_socket,
                    ts_exchange: *ts_exchange,
                    trade: TradePrint {
                        trade_id: trade_id.clone(),
                        price: Fixed::from_raw(*price),
                        quantity: Fixed::from_raw(*quantity),
                        side: side.clone(),
                        ts_exchange: *ts_exchange,
                    },
                },
                ctx,
            );
            report.trade_events += 1;
            report.events_emitted += 1;
        }
        NormEvent::Top {
            seq,
            ts_socket,
            ts_exchange,
            bid,
            ask,
        } => {
            ctx.set_now(*ts_socket);
            // Every successful topbook parse fills all four columns, so a
            // partially-null row is as contentless as a fully-null one.
            let (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) = (bid, ask) else {
                report.skipped_unparseable += 1;
                return Ok(());
            };
            strategy.on_top_of_book(
                &TopBookEvent {
                    seq: *seq,
                    ts_socket: *ts_socket,
                    ts_exchange: *ts_exchange,
                    top: TopOfBook {
                        best_bid: Some(Fixed::from_raw(*bid_price)),
                        best_bid_qty: Some(Fixed::from_raw(*bid_qty)),
                        best_ask: Some(Fixed::from_raw(*ask_price)),
                        best_ask_qty: Some(Fixed::from_raw(*ask_qty)),
                        ts_exchange: *ts_exchange,
                    },
                },
                ctx,
            );
            report.top_book_events += 1;
            report.events_emitted += 1;
        }
    }
    Ok(())
}

fn to_levels(levels: &[(i128, i128)]) -> Vec<Level> {
    levels
        .iter()
        .map(|(price, quantity)| Level {
            price: Fixed::from_raw(*price),
            quantity: Fixed::from_raw(*quantity),
        })
        .collect()
}

fn parquet_files(output: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![output.to_owned()];
    while let Some(directory) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                found.push(path);
            }
        }
    }
    found
}

fn table_of(path: &Path) -> Result<&str, ReplayError> {
    for component in path.components() {
        let name = component.as_os_str().to_str().unwrap_or("");
        if name == "channel=trade" {
            return Ok("trade");
        }
        if name == "channel=book_ticker" {
            return Ok("topbook");
        }
        if name == "channel=book_diff" {
            return Ok("diff");
        }
    }
    Err(ReplayError::Malformed(format!(
        "parquet file without a channel partition: {}",
        path.display()
    )))
}

fn read_table_events(path: &Path, events: &mut Vec<NormEvent>) -> Result<(), ReplayError> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
        ReplayError::Malformed(format!("{}: unreadable parquet: {error}", path.display()))
    })?;
    let version = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.key == SCHEMA_VERSION_KEY)
                .and_then(|entry| entry.value.clone())
        });
    if version.as_deref() != Some(NORMALIZED_SCHEMA_VERSION) {
        return Err(ReplayError::Malformed(format!(
            "{}: unsupported normalized schema version {version:?}, this build reads {NORMALIZED_SCHEMA_VERSION}",
            path.display()
        )));
    }
    let reader = builder
        .build()
        .map_err(|error| ReplayError::Malformed(format!("{}: {error}", path.display())))?;

    match table_of(path)? {
        "diff" => {
            for batch in reader {
                let batch = batch.map_err(|error| {
                    ReplayError::Malformed(format!("{}: {error}", path.display()))
                })?;
                read_diff_batch(&batch, events)?;
            }
        }
        "trade" => {
            for batch in reader {
                let batch = batch.map_err(|error| {
                    ReplayError::Malformed(format!("{}: {error}", path.display()))
                })?;
                read_trade_batch(&batch, events)?;
            }
        }
        _ => {
            for batch in reader {
                let batch = batch.map_err(|error| {
                    ReplayError::Malformed(format!("{}: {error}", path.display()))
                })?;
                read_top_batch(&batch, events)?;
            }
        }
    }
    Ok(())
}

fn read_diff_batch(batch: &RecordBatch, events: &mut Vec<NormEvent>) -> Result<(), ReplayError> {
    let seq = u64_column(batch, "seq")?;
    let ts_socket = i64_column(batch, "ts_socket")?;
    let ts_exchange = opt_i64_column(batch, "ts_exchange")?;
    let first = opt_u64_column(batch, "first_update_id")?;
    let last = opt_u64_column(batch, "last_update_id")?;
    let bids = levels_column(batch, "bids")?;
    let asks = levels_column(batch, "asks")?;
    let synthetic = bool_column(batch, "synthetic")?;
    let gap_reason = opt_str_column(batch, "gap_reason")?;
    let gap_attempts = opt_u32_column(batch, "gap_attempts")?;
    let gap_started = opt_i64_column(batch, "gap_started")?;
    let gap_ended = opt_i64_column(batch, "gap_ended")?;

    for index in 0..batch.num_rows() {
        let ts = Timestamp::from_unix_nanos(ts_socket[index]);
        if synthetic[index] {
            events.push(NormEvent::Gap {
                seq: seq[index],
                ts_socket: ts,
                marker: gap_marker(
                    &gap_reason[index],
                    gap_attempts[index],
                    gap_started[index],
                    gap_ended[index],
                )?,
            });
            continue;
        }
        let span = match (first[index], last[index]) {
            (Some(first), Some(last)) => Some((first, last)),
            _ => None,
        };
        let levels = match (&bids[index], &asks[index]) {
            (Some(bids), Some(asks)) => Some((bids.clone(), asks.clone())),
            _ => None,
        };
        events.push(NormEvent::Diff {
            seq: seq[index],
            ts_socket: ts,
            ts_exchange: ts_exchange[index].map(Timestamp::from_unix_nanos),
            span,
            levels,
        });
    }
    Ok(())
}

fn read_trade_batch(batch: &RecordBatch, events: &mut Vec<NormEvent>) -> Result<(), ReplayError> {
    let seq = u64_column(batch, "seq")?;
    let print_index = u32_column(batch, "print_index")?;
    let ts_socket = i64_column(batch, "ts_socket")?;
    let ts_exchange = opt_i64_column(batch, "ts_exchange")?;
    let trade_id = opt_str_column(batch, "trade_id")?;
    let price = opt_decimal_column(batch, "price")?;
    let quantity = opt_decimal_column(batch, "quantity")?;
    let side = opt_str_column(batch, "side")?;
    let synthetic = bool_column(batch, "synthetic")?;
    let gap_reason = opt_str_column(batch, "gap_reason")?;
    let gap_attempts = opt_u32_column(batch, "gap_attempts")?;
    let gap_started = opt_i64_column(batch, "gap_started")?;
    let gap_ended = opt_i64_column(batch, "gap_ended")?;

    for index in 0..batch.num_rows() {
        let ts = Timestamp::from_unix_nanos(ts_socket[index]);
        if synthetic[index] {
            events.push(NormEvent::Gap {
                seq: seq[index],
                ts_socket: ts,
                marker: gap_marker(
                    &gap_reason[index],
                    gap_attempts[index],
                    gap_started[index],
                    gap_ended[index],
                )?,
            });
            continue;
        }
        events.push(NormEvent::Trade {
            seq: seq[index],
            print_index: print_index[index],
            ts_socket: ts,
            ts_exchange: ts_exchange[index].map(Timestamp::from_unix_nanos),
            trade_id: trade_id[index].clone(),
            price: price[index],
            quantity: quantity[index],
            side: side[index].clone(),
        });
    }
    Ok(())
}

fn read_top_batch(batch: &RecordBatch, events: &mut Vec<NormEvent>) -> Result<(), ReplayError> {
    let seq = u64_column(batch, "seq")?;
    let ts_socket = i64_column(batch, "ts_socket")?;
    let ts_exchange = opt_i64_column(batch, "ts_exchange")?;
    let best_bid = opt_decimal_column(batch, "best_bid")?;
    let best_bid_qty = opt_decimal_column(batch, "best_bid_qty")?;
    let best_ask = opt_decimal_column(batch, "best_ask")?;
    let best_ask_qty = opt_decimal_column(batch, "best_ask_qty")?;
    let synthetic = bool_column(batch, "synthetic")?;
    let gap_reason = opt_str_column(batch, "gap_reason")?;
    let gap_attempts = opt_u32_column(batch, "gap_attempts")?;
    let gap_started = opt_i64_column(batch, "gap_started")?;
    let gap_ended = opt_i64_column(batch, "gap_ended")?;

    for index in 0..batch.num_rows() {
        let ts = Timestamp::from_unix_nanos(ts_socket[index]);
        if synthetic[index] {
            events.push(NormEvent::Gap {
                seq: seq[index],
                ts_socket: ts,
                marker: gap_marker(
                    &gap_reason[index],
                    gap_attempts[index],
                    gap_started[index],
                    gap_ended[index],
                )?,
            });
            continue;
        }
        let bid = match (best_bid[index], best_bid_qty[index]) {
            (Some(price), Some(quantity)) => Some((price, quantity)),
            _ => None,
        };
        let ask = match (best_ask[index], best_ask_qty[index]) {
            (Some(price), Some(quantity)) => Some((price, quantity)),
            _ => None,
        };
        events.push(NormEvent::Top {
            seq: seq[index],
            ts_socket: ts,
            ts_exchange: ts_exchange[index].map(Timestamp::from_unix_nanos),
            bid,
            ask,
        });
    }
    Ok(())
}

fn gap_marker(
    reason: &Option<String>,
    attempts: Option<u32>,
    started: Option<i64>,
    ended: Option<i64>,
) -> Result<GapMarker, ReplayError> {
    match (reason, attempts, started, ended) {
        (Some(reason), Some(attempts), Some(started), Some(ended)) => Ok(GapMarker {
            started_at: Timestamp::from_unix_nanos(started),
            ended_at: Timestamp::from_unix_nanos(ended),
            attempts,
            reason: reason.clone(),
        }),
        _ => Err(ReplayError::Malformed(
            "synthetic row without complete gap fields".to_owned(),
        )),
    }
}

fn column<'batch>(
    batch: &'batch RecordBatch,
    name: &str,
) -> Result<&'batch dyn Array, ReplayError> {
    batch
        .column_by_name(name)
        .map(|array| array.as_ref() as &dyn Array)
        .ok_or_else(|| ReplayError::Malformed(format!("normalized table is missing column {name}")))
}

fn u64_column(batch: &RecordBatch, name: &str) -> Result<Vec<u64>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not uint64")))?;
    Ok((0..array.len()).map(|index| array.value(index)).collect())
}

fn u32_column(batch: &RecordBatch, name: &str) -> Result<Vec<u32>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not uint32")))?;
    Ok((0..array.len()).map(|index| array.value(index)).collect())
}

fn i64_column(batch: &RecordBatch, name: &str) -> Result<Vec<i64>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not int64")))?;
    Ok((0..array.len()).map(|index| array.value(index)).collect())
}

fn bool_column(batch: &RecordBatch, name: &str) -> Result<Vec<bool>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not boolean")))?;
    Ok((0..array.len()).map(|index| array.value(index)).collect())
}

fn opt_u64_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<u64>>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not uint64")))?;
    Ok((0..array.len())
        .map(|index| (!array.is_null(index)).then(|| array.value(index)))
        .collect())
}

fn opt_u32_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<u32>>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not uint32")))?;
    Ok((0..array.len())
        .map(|index| (!array.is_null(index)).then(|| array.value(index)))
        .collect())
}

fn opt_i64_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<i64>>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not int64")))?;
    Ok((0..array.len())
        .map(|index| (!array.is_null(index)).then(|| array.value(index)))
        .collect())
}

fn opt_str_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<String>>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not utf8")))?;
    Ok((0..array.len())
        .map(|index| (!array.is_null(index)).then(|| array.value(index).to_owned()))
        .collect())
}

fn opt_decimal_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<i128>>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not decimal128")))?;
    Ok((0..array.len())
        .map(|index| (!array.is_null(index)).then(|| array.value(index)))
        .collect())
}

fn levels_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<BookLevels>>, ReplayError> {
    let array = column(batch, name)?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| ReplayError::Malformed(format!("column {name} is not a list")))?;
    let mut out = Vec::with_capacity(array.len());
    for index in 0..array.len() {
        if array.is_null(index) {
            out.push(None);
            continue;
        }
        let structure = array.value(index);
        let structure = structure
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                ReplayError::Malformed(format!("column {name} holds no level structs"))
            })?;
        let prices = structure
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| {
                ReplayError::Malformed(format!("column {name} prices are not decimal128"))
            })?;
        let quantities = structure
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| {
                ReplayError::Malformed(format!("column {name} quantities are not decimal128"))
            })?;
        let mut levels = Vec::with_capacity(structure.len());
        for position in 0..structure.len() {
            if structure.is_null(position) {
                continue;
            }
            levels.push((prices.value(position), quantities.value(position)));
        }
        out.push(Some(levels));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use astra_types::{
        CaptureFlags, CaptureId, CaptureManifest, Channel, Instrument, MarketType, Symbol, Venue,
    };
    use std::path::PathBuf;

    use crate::replay::{BookDiffEvent, SnapshotEvent, TopBookEvent, TradeEvent};

    const DEPTH_FRAME: &str = r#"{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":100,"u":105,"b":[["108105.12000000","0.00100000"]],"a":[["108105.13000000","0.00200000"]]}"#;
    const NEXT_FRAME: &str = r#"{"e":"depthUpdate","E":1700000000100,"s":"BTCUSDT","U":106,"u":110,"b":[["108105.11000000","0"]],"a":[]}"#;
    const TRADE_BUNDLE: &str = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672304486868,"data":[{"T":1672304486865,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"16578.50","i":"aaa","seq":1},{"T":1672304486866,"s":"BTCUSDT","S":"Sell","v":"0.002","p":"16578.51","i":"bbb","seq":2}]}"#;
    const BOOK_TICKER: &str = r#"{"u":101057329065,"s":"BTCUSDT","b":"86086.00000000","B":"8.17272000","a":"86086.01000000","A":"0.02862000"}"#;

    struct Counter {
        diffs: u64,
        snapshots: u64,
        trades: u64,
        topbooks: u64,
        gaps: u64,
        ends: u64,
    }

    impl Counter {
        fn plain() -> Self {
            Counter {
                diffs: 0,
                snapshots: 0,
                trades: 0,
                topbooks: 0,
                gaps: 0,
                ends: 0,
            }
        }
    }

    impl Strategy for Counter {
        fn on_book_diff(&mut self, event: &BookDiffEvent, ctx: &mut Context) {
            self.diffs += 1;
            ctx.emit("diff", event.seq.to_le_bytes().to_vec());
        }

        fn on_snapshot(&mut self, event: &SnapshotEvent, ctx: &mut Context) {
            self.snapshots += 1;
            ctx.emit("snapshot", event.seq.to_le_bytes().to_vec());
        }

        fn on_trade(&mut self, event: &TradeEvent, ctx: &mut Context) {
            self.trades += 1;
            let mut payload = Vec::with_capacity(20);
            payload.extend_from_slice(&event.seq.to_le_bytes());
            payload.extend_from_slice(&event.print_index.to_le_bytes());
            ctx.emit("trade", payload);
        }

        fn on_top_of_book(&mut self, event: &TopBookEvent, ctx: &mut Context) {
            self.topbooks += 1;
            ctx.emit("topbook", event.seq.to_le_bytes().to_vec());
        }

        fn on_gap(&mut self, marker: &GapMarker, ctx: &mut Context) {
            self.gaps += 1;
            ctx.emit("gap", marker.reason.as_bytes().to_vec());
        }

        fn on_end(&mut self, _frames: u64, _ctx: &mut Context) {
            self.ends += 1;
        }
    }

    fn spot() -> Instrument {
        Instrument::new(
            Venue::Binance,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    fn bybit_spot() -> Instrument {
        Instrument::new(
            Venue::Bybit,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    fn temp_directory(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("astra-normalized-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn channel_record(
        seq: u64,
        instrument: &Instrument,
        channel: Channel,
        payload: Vec<u8>,
    ) -> astra_types::CaptureRecord {
        astra_types::CaptureRecord {
            seq,
            instrument: instrument.clone(),
            channel,
            ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000 + seq as i64),
            ts_exchange: None,
            payload,
            flags: CaptureFlags::NONE,
        }
    }

    /// Seven frames exercising every path: diffs, a two-print trade bundle,
    /// a topbook, a skipped channel, a gap marker, and an unparseable frame
    /// (the null row that must replay as a skip).
    fn mixed_records() -> Vec<astra_types::CaptureRecord> {
        let marker = GapMarker {
            started_at: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
            ended_at: Timestamp::from_unix_nanos(1_700_000_000_100_000_000),
            attempts: 1,
            reason: "venue_close".to_owned(),
        };
        let mut gap = channel_record(
            4,
            &spot(),
            Channel::BookDiff,
            serde_json::to_vec(&marker).unwrap(),
        );
        gap.flags = CaptureFlags::SYNTHETIC
            .union(CaptureFlags::SEQUENCE_GAP)
            .union(CaptureFlags::UNRELIABLE);
        vec![
            channel_record(
                0,
                &spot(),
                Channel::BookDiff,
                DEPTH_FRAME.as_bytes().to_vec(),
            ),
            channel_record(
                1,
                &bybit_spot(),
                Channel::Trade,
                TRADE_BUNDLE.as_bytes().to_vec(),
            ),
            channel_record(
                2,
                &spot(),
                Channel::BookTicker,
                BOOK_TICKER.as_bytes().to_vec(),
            ),
            channel_record(3, &spot(), Channel::Funding, b"{}".to_vec()),
            gap,
            channel_record(5, &spot(), Channel::BookDiff, b"not json".to_vec()),
            channel_record(
                6,
                &spot(),
                Channel::BookDiff,
                NEXT_FRAME.as_bytes().to_vec(),
            ),
        ]
    }

    fn write_capture(output: &Path, records: &[astra_types::CaptureRecord]) {
        use astra_record::capture::{FRAMES_DIR, write_manifest};
        use astra_record::store::ChunkWriter;

        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();
        let mut writer = ChunkWriter::open(output.join(FRAMES_DIR), 100).unwrap();
        for record in records {
            writer.append(record).unwrap();
        }
        writer.finish().unwrap();

        let venue_frames = records
            .iter()
            .filter(|record| !record.flags.contains(CaptureFlags::SYNTHETIC))
            .count() as u64;
        let manifest = CaptureManifest {
            schema_version: astra_types::SCHEMA_VERSION,
            capture_id: CaptureId::new("normalized-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: spot(),
            channel: Channel::BookDiff,
            frames_written: venue_frames,
            stop_reason: None,
        };
        write_manifest(output, &manifest).unwrap();
    }

    #[test]
    fn raw_and_normalized_replay_emit_identical_signals() {
        let capture = temp_directory("unified-capture");
        let tables = temp_directory("unified-tables");
        write_capture(&capture, &mixed_records());
        astra_normalize::normalize(&capture, &tables).unwrap();

        let mut raw_strategy = Counter::plain();
        let raw = crate::replay::replay(&capture, 7, &mut raw_strategy).unwrap();

        let mut norm_strategy = Counter::plain();
        let normalized = replay_normalized(&tables, 7, &mut norm_strategy).unwrap();

        // The signals are the verdict: identical hashes mean every event —
        // diffs, both trade prints with their indices, topbook, gap —
        // arrived with identical content in identical order.
        assert_eq!(raw.signal_hash, normalized.signal_hash);
        assert_eq!(raw.signals, normalized.signals);
        assert_eq!(raw.signals, 6);
        assert_eq!(norm_strategy.diffs, raw_strategy.diffs);
        assert_eq!(norm_strategy.trades, raw_strategy.trades);
        assert_eq!(norm_strategy.topbooks, raw_strategy.topbooks);
        assert_eq!(norm_strategy.gaps, raw_strategy.gaps);
        assert_eq!(norm_strategy.ends, 1);

        // The null row replays as a counted skip, exactly like raw replay's
        // unparseable frame — this count IS the unification, in the schema.
        assert_eq!(raw.skipped_unparseable, 1);
        assert_eq!(normalized.skipped_unparseable, 1);

        std::fs::remove_dir_all(&capture).unwrap();
        std::fs::remove_dir_all(&tables).unwrap();
    }

    #[test]
    fn normalized_replay_is_seed_independent_like_raw() {
        let capture = temp_directory("unified-seeds");
        let tables = temp_directory("unified-seeds-tables");
        write_capture(&capture, &mixed_records());
        astra_normalize::normalize(&capture, &tables).unwrap();

        let mut first = Counter::plain();
        let mut second = Counter::plain();
        let one = replay_normalized(&tables, 7, &mut first).unwrap();
        let two = replay_normalized(&tables, 99, &mut second).unwrap();
        assert_eq!(one.signal_hash, two.signal_hash);

        std::fs::remove_dir_all(&capture).unwrap();
        std::fs::remove_dir_all(&tables).unwrap();
    }

    #[test]
    fn stray_parquet_without_a_channel_is_refused_not_guessed() {
        let tables = temp_directory("unified-stray");
        let capture = temp_directory("unified-stray-capture");
        write_capture(&capture, &mixed_records());
        astra_normalize::normalize(&capture, &tables).unwrap();

        // Move one table out of its Hive partition: the reader must fail
        // loud instead of replaying a table it cannot identify.
        let stray = tables.join("stray.parquet");
        let topbook = parquet_files(&tables)
            .into_iter()
            .find(|path| path.to_str().unwrap().contains("channel=book_ticker"))
            .unwrap();
        std::fs::rename(&topbook, &stray).unwrap();

        let mut strategy = Counter::plain();
        assert!(matches!(
            replay_normalized(&tables, 7, &mut strategy),
            Err(ReplayError::Malformed(_))
        ));

        std::fs::remove_dir_all(&capture).unwrap();
        std::fs::remove_dir_all(&tables).unwrap();
    }
}
