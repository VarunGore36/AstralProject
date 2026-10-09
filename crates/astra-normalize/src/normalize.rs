use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, BooleanBuilder, Decimal128Builder, Int64Builder, ListBuilder, StringBuilder,
    StructBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;

use astra_record::capture::{FRAMES_DIR, MANIFEST_FILE};
use astra_record::feed;
use astra_record::store;
use astra_types::{CaptureFlags, CaptureManifest, Channel, GapMarker};

pub const SCHEMA_VERSION: u32 = 1;
pub const DECIMAL_PRECISION: u8 = 20;
pub const DECIMAL_SCALE: i8 = 8;
/// Largest value a `Decimal128(20, 8)` column can hold, in raw units.
/// Anything larger parses as `Fixed` but does not fit the column.
const DECIMAL_MAX_RAW: i128 = 99_999_999_999_999_999_999;
const SCHEMA_VERSION_KEY: &str = "astra.schema_version";

#[derive(Debug, Error)]
pub enum NormalizeError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("store error: {0}")]
    Store(#[from] store::StoreError),
    #[error("manifest error: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("validation rejected the batch: {0}")]
    Validation(String),
    #[error("unsupported capture schema version {found}, this build reads {expected}")]
    SchemaVersion { found: u32, expected: u32 },
}

#[derive(Clone, Debug, Default)]
pub struct NormalizeSummary {
    pub records: u64,
    pub rows_written: u64,
    pub skipped_unsupported_channel: u64,
    pub files: Vec<PathBuf>,
}

pub fn normalize(input: &Path, output: &Path) -> Result<NormalizeSummary, NormalizeError> {
    let manifest = read_manifest(input)?;
    let records = store::read_all(&input.join(FRAMES_DIR))?;

    let venue_frames: u64 = records
        .iter()
        .filter(|record| !record.flags.contains(CaptureFlags::SYNTHETIC))
        .count() as u64;
    if venue_frames != manifest.frames_written {
        return Err(NormalizeError::Validation(format!(
            "manifest claims {} venue frames but the capture holds {}",
            manifest.frames_written, venue_frames
        )));
    }

    let mut diff_rows = Vec::with_capacity(records.len());
    let mut trade_rows = Vec::new();
    let mut top_book_rows = Vec::new();
    let mut skipped_unsupported_channel = 0u64;

    for record in &records {
        match record.channel {
            Channel::BookDiff => diff_rows.push(book_diff_row(&manifest, record)?),
            Channel::Trade => trade_rows.extend(trade_rows_for(&manifest, record)?),
            Channel::BookTicker => top_book_rows.push(top_book_row(&manifest, record)?),
            _ => skipped_unsupported_channel += 1,
        }
    }

    validate_batch(&diff_rows)?;
    validate_trades(&trade_rows)?;
    validate_top_books(&top_book_rows)?;

    let mut files = Vec::new();
    files.extend(write_grouped(output, &diff_rows, |path, rows| {
        write_batch(path, rows)
    })?);
    files.extend(write_grouped(output, &trade_rows, |path, rows| {
        write_trade_batch(path, rows)
    })?);
    files.extend(write_grouped(output, &top_book_rows, |path, rows| {
        write_top_book_batch(path, rows)
    })?);

    Ok(NormalizeSummary {
        records: records.len() as u64,
        rows_written: (diff_rows.len() + trade_rows.len() + top_book_rows.len()) as u64,
        skipped_unsupported_channel,
        files,
    })
}

fn write_grouped<T, W>(output: &Path, rows: &[T], write: W) -> Result<Vec<PathBuf>, NormalizeError>
where
    T: Partitioned,
    W: Fn(&Path, &[&T]) -> Result<(), NormalizeError>,
{
    let mut by_date: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        by_date
            .entry(date_of(row.ts_socket()))
            .or_default()
            .push(index);
    }

    let mut files = Vec::new();
    let mut dates: Vec<String> = by_date.keys().cloned().collect();
    dates.sort();

    for date in dates {
        let indices = &by_date[&date];
        let batch_rows: Vec<&T> = indices.iter().map(|index| &rows[*index]).collect();
        let path = part_path(
            output,
            batch_rows[0].venue(),
            batch_rows[0].market_type(),
            batch_rows[0].symbol(),
            batch_rows[0].channel(),
            &date,
        )?;
        write(&path, &batch_rows)?;
        files.push(path);
    }

    Ok(files)
}

trait Partitioned {
    fn ts_socket(&self) -> i64;
    fn venue(&self) -> &str;
    fn market_type(&self) -> &str;
    fn symbol(&self) -> &str;
    fn channel(&self) -> &str;
}

impl Partitioned for BookDiffRow {
    fn ts_socket(&self) -> i64 {
        self.ts_socket
    }
    fn venue(&self) -> &str {
        &self.venue
    }
    fn market_type(&self) -> &str {
        &self.market_type
    }
    fn symbol(&self) -> &str {
        &self.symbol
    }
    fn channel(&self) -> &str {
        &self.channel
    }
}

impl Partitioned for TradeRow {
    fn ts_socket(&self) -> i64 {
        self.ts_socket
    }
    fn venue(&self) -> &str {
        &self.venue
    }
    fn market_type(&self) -> &str {
        &self.market_type
    }
    fn symbol(&self) -> &str {
        &self.symbol
    }
    fn channel(&self) -> &str {
        &self.channel
    }
}

struct BookDiffRow {
    venue: String,
    market_type: String,
    symbol: String,
    channel: String,
    ts_exchange: Option<i64>,
    ts_socket: i64,
    first_update_id: Option<u64>,
    last_update_id: Option<u64>,
    bids: Option<Vec<(i128, i128)>>,
    asks: Option<Vec<(i128, i128)>>,
    capture_id: String,
    seq: u64,
    flags: u32,
    synthetic: bool,
    gap_reason: Option<String>,
    gap_attempts: Option<u32>,
    gap_started: Option<i64>,
    gap_ended: Option<i64>,
}

fn book_diff_row(
    manifest: &CaptureManifest,
    record: &astra_types::CaptureRecord,
) -> Result<BookDiffRow, NormalizeError> {
    let venue = record.instrument.venue();
    let mut row = BookDiffRow {
        venue: venue.to_string(),
        market_type: record.instrument.market_type().to_string(),
        symbol: record.instrument.symbol().to_string(),
        channel: record.channel.to_string(),
        ts_exchange: record
            .ts_exchange
            .map(|timestamp| timestamp.unix_nanos())
            .or_else(|| {
                feed::exchange_time(venue, record.channel, &record.payload)
                    .map(|timestamp| timestamp.unix_nanos())
            }),
        ts_socket: record.ts_socket.unix_nanos(),
        first_update_id: None,
        last_update_id: None,
        bids: None,
        asks: None,
        capture_id: manifest.capture_id.as_str().to_owned(),
        seq: record.seq,
        flags: record.flags.bits(),
        synthetic: record.flags.contains(CaptureFlags::SYNTHETIC),
        gap_reason: None,
        gap_attempts: None,
        gap_started: None,
        gap_ended: None,
    };

    if row.synthetic {
        let marker: GapMarker = serde_json::from_slice(&record.payload)?;
        row.gap_reason = Some(marker.reason);
        row.gap_attempts = Some(marker.attempts);
        row.gap_started = Some(marker.started_at.unix_nanos());
        row.gap_ended = Some(marker.ended_at.unix_nanos());
        return Ok(row);
    }

    if let Some(span) = feed::update_span(venue, record.channel, &record.payload) {
        row.first_update_id = Some(span.first);
        row.last_update_id = Some(span.last);
    }

    if let Some(diff) = feed::book_diff(venue, record.channel, &record.payload) {
        row.bids = Some(
            diff.bids
                .iter()
                .map(|level| (level.price.raw(), level.quantity.raw()))
                .collect(),
        );
        row.asks = Some(
            diff.asks
                .iter()
                .map(|level| (level.price.raw(), level.quantity.raw()))
                .collect(),
        );
    }

    Ok(row)
}

fn validate_batch(rows: &[BookDiffRow]) -> Result<(), NormalizeError> {
    let mut seen_seq = HashSet::with_capacity(rows.len());

    for row in rows {
        if !seen_seq.insert(row.seq) {
            return Err(NormalizeError::Validation(format!(
                "duplicate seq {} in capture {}",
                row.seq, row.capture_id
            )));
        }

        if let Some(levels) = row
            .bids
            .as_ref()
            .map(|bids| bids.iter().chain(row.asks.iter().flatten()))
        {
            for (price, quantity) in levels {
                if *price <= 0 {
                    return Err(NormalizeError::Validation(format!(
                        "non-positive price {price} at seq {}",
                        row.seq
                    )));
                }
                if *quantity < 0 {
                    return Err(NormalizeError::Validation(format!(
                        "negative quantity {quantity} at seq {}",
                        row.seq
                    )));
                }
                if *price > DECIMAL_MAX_RAW || *quantity > DECIMAL_MAX_RAW {
                    return Err(NormalizeError::Validation(format!(
                        "value beyond Decimal128(20,8) at seq {}: price {price}, quantity {quantity}",
                        row.seq
                    )));
                }
            }
        }
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct TradeRow {
    venue: String,
    market_type: String,
    symbol: String,
    channel: String,
    ts_exchange: Option<i64>,
    ts_socket: i64,
    trade_id: Option<String>,
    price: Option<i128>,
    quantity: Option<i128>,
    side: Option<String>,
    print_index: u32,
    capture_id: String,
    seq: u64,
    flags: u32,
    synthetic: bool,
    gap_reason: Option<String>,
    gap_attempts: Option<u32>,
    gap_started: Option<i64>,
    gap_ended: Option<i64>,
}

fn trade_rows_for(
    manifest: &CaptureManifest,
    record: &astra_types::CaptureRecord,
) -> Result<Vec<TradeRow>, NormalizeError> {
    let venue = record.instrument.venue();
    let mut base = TradeRow {
        venue: venue.to_string(),
        market_type: record.instrument.market_type().to_string(),
        symbol: record.instrument.symbol().to_string(),
        channel: record.channel.to_string(),
        ts_exchange: record.ts_exchange.map(|timestamp| timestamp.unix_nanos()),
        ts_socket: record.ts_socket.unix_nanos(),
        trade_id: None,
        price: None,
        quantity: None,
        side: None,
        print_index: 0,
        capture_id: manifest.capture_id.as_str().to_owned(),
        seq: record.seq,
        flags: record.flags.bits(),
        synthetic: record.flags.contains(CaptureFlags::SYNTHETIC),
        gap_reason: None,
        gap_attempts: None,
        gap_started: None,
        gap_ended: None,
    };

    if base.synthetic {
        let marker: GapMarker = serde_json::from_slice(&record.payload)?;
        base.gap_reason = Some(marker.reason);
        base.gap_attempts = Some(marker.attempts);
        base.gap_started = Some(marker.started_at.unix_nanos());
        base.gap_ended = Some(marker.ended_at.unix_nanos());
        return Ok(vec![base]);
    }

    let Some(prints) = feed::trade_prints(venue, record.channel, &record.payload) else {
        return Ok(vec![base]);
    };

    Ok(prints
        .into_iter()
        .enumerate()
        .map(|(index, print)| {
            let mut row = TradeRow {
                trade_id: print.trade_id,
                price: Some(print.price.raw()),
                quantity: Some(print.quantity.raw()),
                side: print.side,
                print_index: index as u32,
                ..base.clone()
            };
            if print.ts_exchange.is_some() {
                row.ts_exchange = print.ts_exchange.map(|timestamp| timestamp.unix_nanos());
            }
            row
        })
        .collect())
}

fn validate_trades(rows: &[TradeRow]) -> Result<(), NormalizeError> {
    let mut seen_keys = HashSet::with_capacity(rows.len());

    for row in rows {
        if !seen_keys.insert((row.seq, row.print_index)) {
            return Err(NormalizeError::Validation(format!(
                "duplicate seq {} print {} in capture {}",
                row.seq, row.print_index, row.capture_id
            )));
        }

        if let Some(price) = row.price {
            if price <= 0 {
                return Err(NormalizeError::Validation(format!(
                    "non-positive price {price} at seq {}",
                    row.seq
                )));
            }
        }
        if let Some(quantity) = row.quantity {
            if quantity < 0 {
                return Err(NormalizeError::Validation(format!(
                    "negative quantity {quantity} at seq {}",
                    row.seq
                )));
            }
            if quantity > DECIMAL_MAX_RAW {
                return Err(NormalizeError::Validation(format!(
                    "quantity beyond Decimal128(20,8) at seq {}: {quantity}",
                    row.seq
                )));
            }
        }
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct TopBookRow {
    venue: String,
    market_type: String,
    symbol: String,
    channel: String,
    ts_exchange: Option<i64>,
    ts_socket: i64,
    best_bid: Option<i128>,
    best_bid_qty: Option<i128>,
    best_ask: Option<i128>,
    best_ask_qty: Option<i128>,
    capture_id: String,
    seq: u64,
    flags: u32,
    synthetic: bool,
    gap_reason: Option<String>,
    gap_attempts: Option<u32>,
    gap_started: Option<i64>,
    gap_ended: Option<i64>,
}

impl Partitioned for TopBookRow {
    fn ts_socket(&self) -> i64 {
        self.ts_socket
    }
    fn venue(&self) -> &str {
        &self.venue
    }
    fn market_type(&self) -> &str {
        &self.market_type
    }
    fn symbol(&self) -> &str {
        &self.symbol
    }
    fn channel(&self) -> &str {
        &self.channel
    }
}

fn top_book_row(
    manifest: &CaptureManifest,
    record: &astra_types::CaptureRecord,
) -> Result<TopBookRow, NormalizeError> {
    let venue = record.instrument.venue();
    let mut row = TopBookRow {
        venue: venue.to_string(),
        market_type: record.instrument.market_type().to_string(),
        symbol: record.instrument.symbol().to_string(),
        channel: record.channel.to_string(),
        ts_exchange: record.ts_exchange.map(|timestamp| timestamp.unix_nanos()),
        ts_socket: record.ts_socket.unix_nanos(),
        best_bid: None,
        best_bid_qty: None,
        best_ask: None,
        best_ask_qty: None,
        capture_id: manifest.capture_id.as_str().to_owned(),
        seq: record.seq,
        flags: record.flags.bits(),
        synthetic: record.flags.contains(CaptureFlags::SYNTHETIC),
        gap_reason: None,
        gap_attempts: None,
        gap_started: None,
        gap_ended: None,
    };

    if row.synthetic {
        let marker: GapMarker = serde_json::from_slice(&record.payload)?;
        row.gap_reason = Some(marker.reason);
        row.gap_attempts = Some(marker.attempts);
        row.gap_started = Some(marker.started_at.unix_nanos());
        row.gap_ended = Some(marker.ended_at.unix_nanos());
        return Ok(row);
    }

    if let Some(top) = feed::top_of_book(venue, record.channel, &record.payload) {
        row.best_bid = top.best_bid.map(|price| price.raw());
        row.best_bid_qty = top.best_bid_qty.map(|quantity| quantity.raw());
        row.best_ask = top.best_ask.map(|price| price.raw());
        row.best_ask_qty = top.best_ask_qty.map(|quantity| quantity.raw());
        if top.ts_exchange.is_some() {
            row.ts_exchange = top.ts_exchange.map(|timestamp| timestamp.unix_nanos());
        }
    }

    Ok(row)
}

fn validate_top_books(rows: &[TopBookRow]) -> Result<(), NormalizeError> {
    let mut seen_seq = HashSet::with_capacity(rows.len());

    for row in rows {
        if !seen_seq.insert(row.seq) {
            return Err(NormalizeError::Validation(format!(
                "duplicate seq {} in capture {}",
                row.seq, row.capture_id
            )));
        }

        for (label, value) in [("best_bid", row.best_bid), ("best_ask", row.best_ask)] {
            if matches!(value, Some(price) if price <= 0) {
                return Err(NormalizeError::Validation(format!(
                    "invalid {label} at seq {}",
                    row.seq
                )));
            }
            if let Some(price) = value {
                if price > DECIMAL_MAX_RAW {
                    return Err(NormalizeError::Validation(format!(
                        "{label} beyond Decimal128(20,8) at seq {}: {price}",
                        row.seq
                    )));
                }
            }
        }
        for (label, value) in [
            ("best_bid_qty", row.best_bid_qty),
            ("best_ask_qty", row.best_ask_qty),
        ] {
            if matches!(value, Some(quantity) if quantity < 0) {
                return Err(NormalizeError::Validation(format!(
                    "invalid {label} at seq {}",
                    row.seq
                )));
            }
            if let Some(quantity) = value {
                if quantity > DECIMAL_MAX_RAW {
                    return Err(NormalizeError::Validation(format!(
                        "{label} beyond Decimal128(20,8) at seq {}: {quantity}",
                        row.seq
                    )));
                }
            }
        }
    }

    Ok(())
}

fn date_of(ts_socket: i64) -> String {
    DateTime::<Utc>::from_timestamp_nanos(ts_socket)
        .date_naive()
        .to_string()
}

fn part_path(
    output: &Path,
    venue: &str,
    market_type: &str,
    symbol: &str,
    channel: &str,
    date: &str,
) -> Result<PathBuf, NormalizeError> {
    let symbol = symbol.replace('/', "_");
    let directory = output
        .join(format!("venue={venue}"))
        .join(format!("market_type={market_type}"))
        .join(format!("symbol={symbol}"))
        .join(format!("channel={channel}"))
        .join(format!("date={date}"));
    std::fs::create_dir_all(&directory)?;

    let existing = std::fs::read_dir(&directory)?
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("part-") && name.ends_with(".parquet"))
        })
        .count();

    Ok(directory.join(format!("part-{existing:05}.parquet")))
}

fn book_diff_schema() -> Schema {
    let decimal = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);
    let level_fields: Fields = vec![
        Field::new("price", decimal.clone(), false),
        Field::new("quantity", decimal.clone(), false),
    ]
    .into();
    let level_struct = DataType::Struct(level_fields);

    Schema::new(vec![
        Field::new("venue", DataType::Utf8, false),
        Field::new("market_type", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts_exchange", DataType::Int64, true),
        Field::new("ts_socket", DataType::Int64, false),
        Field::new("ts_ready", DataType::Int64, true),
        Field::new("first_update_id", DataType::UInt64, true),
        Field::new("last_update_id", DataType::UInt64, true),
        Field::new(
            "bids",
            DataType::List(Field::new("item", level_struct.clone(), true).into()),
            true,
        ),
        Field::new(
            "asks",
            DataType::List(Field::new("item", level_struct, true).into()),
            true,
        ),
        Field::new("capture_id", DataType::Utf8, false),
        Field::new("seq", DataType::UInt64, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("synthetic", DataType::Boolean, false),
        Field::new("gap_reason", DataType::Utf8, true),
        Field::new("gap_attempts", DataType::UInt32, true),
        Field::new("gap_started", DataType::Int64, true),
        Field::new("gap_ended", DataType::Int64, true),
    ])
}

fn write_batch(path: &Path, rows: &[&BookDiffRow]) -> Result<(), NormalizeError> {
    let schema = book_diff_schema();
    let decimal = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);

    let mut venue = StringBuilder::new();
    let mut market_type = StringBuilder::new();
    let mut symbol = StringBuilder::new();
    let mut ts_exchange = Int64Builder::new();
    let mut ts_socket = Int64Builder::new();
    let mut ts_ready = Int64Builder::new();
    let mut first_update_id = UInt64Builder::new();
    let mut last_update_id = UInt64Builder::new();
    let mut bids = level_list_builder(&decimal);
    let mut asks = level_list_builder(&decimal);
    let mut capture_id = StringBuilder::new();
    let mut seq = UInt64Builder::new();
    let mut flags = UInt32Builder::new();
    let mut synthetic = BooleanBuilder::new();
    let mut gap_reason = StringBuilder::new();
    let mut gap_attempts = UInt32Builder::new();
    let mut gap_started = Int64Builder::new();
    let mut gap_ended = Int64Builder::new();

    for row in rows {
        venue.append_value(&row.venue);
        market_type.append_value(&row.market_type);
        symbol.append_value(&row.symbol);
        append_optional(&mut ts_exchange, row.ts_exchange);
        ts_socket.append_value(row.ts_socket);
        ts_ready.append_null();
        append_optional(&mut first_update_id, row.first_update_id);
        append_optional(&mut last_update_id, row.last_update_id);
        append_levels(&mut bids, row.bids.as_deref());
        append_levels(&mut asks, row.asks.as_deref());
        capture_id.append_value(&row.capture_id);
        seq.append_value(row.seq);
        flags.append_value(row.flags);
        synthetic.append_value(row.synthetic);
        match row.gap_reason.as_deref() {
            Some(reason) => gap_reason.append_value(reason),
            None => gap_reason.append_null(),
        }
        append_optional(&mut gap_attempts, row.gap_attempts);
        append_optional(&mut gap_started, row.gap_started);
        append_optional(&mut gap_ended, row.gap_ended);
    }

    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(venue.finish()),
            Arc::new(market_type.finish()),
            Arc::new(symbol.finish()),
            Arc::new(ts_exchange.finish()),
            Arc::new(ts_socket.finish()),
            Arc::new(ts_ready.finish()),
            Arc::new(first_update_id.finish()),
            Arc::new(last_update_id.finish()),
            Arc::new(bids.finish()),
            Arc::new(asks.finish()),
            Arc::new(capture_id.finish()),
            Arc::new(seq.finish()),
            Arc::new(flags.finish()),
            Arc::new(synthetic.finish()),
            Arc::new(gap_reason.finish()),
            Arc::new(gap_attempts.finish()),
            Arc::new(gap_started.finish()),
            Arc::new(gap_ended.finish()),
        ],
    )?;

    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![parquet_metadata()]))
        .build();
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn parquet_metadata() -> parquet::file::metadata::KeyValue {
    parquet::file::metadata::KeyValue::new(
        SCHEMA_VERSION_KEY.to_owned(),
        SCHEMA_VERSION.to_string(),
    )
}

fn trade_schema() -> Schema {
    let decimal = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);

    Schema::new(vec![
        Field::new("venue", DataType::Utf8, false),
        Field::new("market_type", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts_exchange", DataType::Int64, true),
        Field::new("ts_socket", DataType::Int64, false),
        Field::new("ts_ready", DataType::Int64, true),
        Field::new("trade_id", DataType::Utf8, true),
        Field::new("price", decimal.clone(), true),
        Field::new("quantity", decimal, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("print_index", DataType::UInt32, false),
        Field::new("capture_id", DataType::Utf8, false),
        Field::new("seq", DataType::UInt64, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("synthetic", DataType::Boolean, false),
        Field::new("gap_reason", DataType::Utf8, true),
        Field::new("gap_attempts", DataType::UInt32, true),
        Field::new("gap_started", DataType::Int64, true),
        Field::new("gap_ended", DataType::Int64, true),
    ])
}

fn write_trade_batch(path: &Path, rows: &[&TradeRow]) -> Result<(), NormalizeError> {
    let schema = trade_schema();

    let mut venue = StringBuilder::new();
    let mut market_type = StringBuilder::new();
    let mut symbol = StringBuilder::new();
    let mut ts_exchange = Int64Builder::new();
    let mut ts_socket = Int64Builder::new();
    let mut ts_ready = Int64Builder::new();
    let mut trade_id = StringBuilder::new();
    let mut price = Decimal128Builder::new()
        .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE));
    let mut quantity = Decimal128Builder::new()
        .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE));
    let mut side = StringBuilder::new();
    let mut print_index = UInt32Builder::new();
    let mut capture_id = StringBuilder::new();
    let mut seq = UInt64Builder::new();
    let mut flags = UInt32Builder::new();
    let mut synthetic = BooleanBuilder::new();
    let mut gap_reason = StringBuilder::new();
    let mut gap_attempts = UInt32Builder::new();
    let mut gap_started = Int64Builder::new();
    let mut gap_ended = Int64Builder::new();

    for row in rows {
        venue.append_value(&row.venue);
        market_type.append_value(&row.market_type);
        symbol.append_value(&row.symbol);
        append_optional(&mut ts_exchange, row.ts_exchange);
        ts_socket.append_value(row.ts_socket);
        ts_ready.append_null();
        match row.trade_id.as_deref() {
            Some(id) => trade_id.append_value(id),
            None => trade_id.append_null(),
        }
        append_optional_decimal(&mut price, row.price);
        append_optional_decimal(&mut quantity, row.quantity);
        match row.side.as_deref() {
            Some(side_value) => side.append_value(side_value),
            None => side.append_null(),
        }
        print_index.append_value(row.print_index);
        capture_id.append_value(&row.capture_id);
        seq.append_value(row.seq);
        flags.append_value(row.flags);
        synthetic.append_value(row.synthetic);
        match row.gap_reason.as_deref() {
            Some(reason) => gap_reason.append_value(reason),
            None => gap_reason.append_null(),
        }
        append_optional(&mut gap_attempts, row.gap_attempts);
        append_optional(&mut gap_started, row.gap_started);
        append_optional(&mut gap_ended, row.gap_ended);
    }

    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(venue.finish()),
            Arc::new(market_type.finish()),
            Arc::new(symbol.finish()),
            Arc::new(ts_exchange.finish()),
            Arc::new(ts_socket.finish()),
            Arc::new(ts_ready.finish()),
            Arc::new(trade_id.finish()),
            Arc::new(price.finish()),
            Arc::new(quantity.finish()),
            Arc::new(side.finish()),
            Arc::new(print_index.finish()),
            Arc::new(capture_id.finish()),
            Arc::new(seq.finish()),
            Arc::new(flags.finish()),
            Arc::new(synthetic.finish()),
            Arc::new(gap_reason.finish()),
            Arc::new(gap_attempts.finish()),
            Arc::new(gap_started.finish()),
            Arc::new(gap_ended.finish()),
        ],
    )?;

    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![parquet_metadata()]))
        .build();
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn append_optional_decimal(builder: &mut Decimal128Builder, value: Option<i128>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn top_book_schema() -> Schema {
    let decimal = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);

    Schema::new(vec![
        Field::new("venue", DataType::Utf8, false),
        Field::new("market_type", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts_exchange", DataType::Int64, true),
        Field::new("ts_socket", DataType::Int64, false),
        Field::new("ts_ready", DataType::Int64, true),
        Field::new("best_bid", decimal.clone(), true),
        Field::new("best_bid_qty", decimal.clone(), true),
        Field::new("best_ask", decimal.clone(), true),
        Field::new("best_ask_qty", decimal, true),
        Field::new("capture_id", DataType::Utf8, false),
        Field::new("seq", DataType::UInt64, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("synthetic", DataType::Boolean, false),
        Field::new("gap_reason", DataType::Utf8, true),
        Field::new("gap_attempts", DataType::UInt32, true),
        Field::new("gap_started", DataType::Int64, true),
        Field::new("gap_ended", DataType::Int64, true),
    ])
}

fn write_top_book_batch(path: &Path, rows: &[&TopBookRow]) -> Result<(), NormalizeError> {
    let schema = top_book_schema();

    let mut venue = StringBuilder::new();
    let mut market_type = StringBuilder::new();
    let mut symbol = StringBuilder::new();
    let mut ts_exchange = Int64Builder::new();
    let mut ts_socket = Int64Builder::new();
    let mut ts_ready = Int64Builder::new();
    let mut best_bid = Decimal128Builder::new()
        .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE));
    let mut best_bid_qty = Decimal128Builder::new()
        .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE));
    let mut best_ask = Decimal128Builder::new()
        .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE));
    let mut best_ask_qty = Decimal128Builder::new()
        .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE));
    let mut capture_id = StringBuilder::new();
    let mut seq = UInt64Builder::new();
    let mut flags = UInt32Builder::new();
    let mut synthetic = BooleanBuilder::new();
    let mut gap_reason = StringBuilder::new();
    let mut gap_attempts = UInt32Builder::new();
    let mut gap_started = Int64Builder::new();
    let mut gap_ended = Int64Builder::new();

    for row in rows {
        venue.append_value(&row.venue);
        market_type.append_value(&row.market_type);
        symbol.append_value(&row.symbol);
        append_optional(&mut ts_exchange, row.ts_exchange);
        ts_socket.append_value(row.ts_socket);
        ts_ready.append_null();
        append_optional_decimal(&mut best_bid, row.best_bid);
        append_optional_decimal(&mut best_bid_qty, row.best_bid_qty);
        append_optional_decimal(&mut best_ask, row.best_ask);
        append_optional_decimal(&mut best_ask_qty, row.best_ask_qty);
        capture_id.append_value(&row.capture_id);
        seq.append_value(row.seq);
        flags.append_value(row.flags);
        synthetic.append_value(row.synthetic);
        match row.gap_reason.as_deref() {
            Some(reason) => gap_reason.append_value(reason),
            None => gap_reason.append_null(),
        }
        append_optional(&mut gap_attempts, row.gap_attempts);
        append_optional(&mut gap_started, row.gap_started);
        append_optional(&mut gap_ended, row.gap_ended);
    }

    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(venue.finish()),
            Arc::new(market_type.finish()),
            Arc::new(symbol.finish()),
            Arc::new(ts_exchange.finish()),
            Arc::new(ts_socket.finish()),
            Arc::new(ts_ready.finish()),
            Arc::new(best_bid.finish()),
            Arc::new(best_bid_qty.finish()),
            Arc::new(best_ask.finish()),
            Arc::new(best_ask_qty.finish()),
            Arc::new(capture_id.finish()),
            Arc::new(seq.finish()),
            Arc::new(flags.finish()),
            Arc::new(synthetic.finish()),
            Arc::new(gap_reason.finish()),
            Arc::new(gap_attempts.finish()),
            Arc::new(gap_started.finish()),
            Arc::new(gap_ended.finish()),
        ],
    )?;

    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![parquet_metadata()]))
        .build();
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn level_list_builder(decimal: &DataType) -> ListBuilder<StructBuilder> {
    let fields: Fields = vec![
        Field::new("price", decimal.clone(), false),
        Field::new("quantity", decimal.clone(), false),
    ]
    .into();
    let builders: Vec<Box<dyn ArrayBuilder>> = vec![
        Box::new(Decimal128Builder::new().with_data_type(decimal.clone())),
        Box::new(Decimal128Builder::new().with_data_type(decimal.clone())),
    ];
    ListBuilder::new(StructBuilder::new(fields, builders))
}

fn append_levels(builder: &mut ListBuilder<StructBuilder>, levels: Option<&[(i128, i128)]>) {
    let Some(levels) = levels else {
        builder.append_null();
        return;
    };

    let values = builder.values();
    for (price, quantity) in levels {
        // Builders are constructed two lines above in this exact order, so
        // these downcasts cannot fail; expect documents the invariant.
        values
            .field_builder::<Decimal128Builder>(0)
            .expect("price builder at index 0")
            .append_value(*price);
        values
            .field_builder::<Decimal128Builder>(1)
            .expect("quantity builder at index 1")
            .append_value(*quantity);
        values.append(true);
    }
    builder.append(true);
}

fn append_optional<T, B>(builder: &mut B, value: Option<T>)
where
    B: ArrayBuilderWithValue<T>,
{
    match value {
        Some(value) => builder.append(value),
        None => builder.append_null_value(),
    }
}

trait ArrayBuilderWithValue<T> {
    fn append(&mut self, value: T);
    fn append_null_value(&mut self);
}

macro_rules! impl_optional_append {
    ($builder:ty, $value:ty, $append:ident) => {
        impl ArrayBuilderWithValue<$value> for $builder {
            fn append(&mut self, value: $value) {
                self.$append(value);
            }
            fn append_null_value(&mut self) {
                self.append_null();
            }
        }
    };
}

impl_optional_append!(Int64Builder, i64, append_value);
impl_optional_append!(UInt64Builder, u64, append_value);
impl_optional_append!(UInt32Builder, u32, append_value);

fn read_manifest(input: &Path) -> Result<CaptureManifest, NormalizeError> {
    let body = std::fs::read_to_string(input.join(MANIFEST_FILE))?;
    let manifest: CaptureManifest = serde_json::from_str(&body)?;
    // A format change must fail loudly here, not misread silently.
    if manifest.schema_version != astra_types::SCHEMA_VERSION {
        return Err(NormalizeError::SchemaVersion {
            found: manifest.schema_version,
            expected: astra_types::SCHEMA_VERSION,
        });
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use astra_types::{
        CaptureId, CaptureRecord, Channel, Fixed, Instrument, MarketType, Symbol, Timestamp, Venue,
    };

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

    const DEPTH_FRAME: &str = r#"{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":100,"u":105,"b":[["108105.12000000","0.00100000"]],"a":[["108105.13000000","0.00200000"]]}"#;
    const NEXT_FRAME: &str = r#"{"e":"depthUpdate","E":1700000000100,"s":"BTCUSDT","U":106,"u":110,"b":[["108105.11000000","0"]],"a":[]}"#;
    const TRADE_FRAME: &str =
        r#"{"e":"trade","E":1700000000000,"s":"BTCUSDT","t":1,"p":"108105.12","q":"0.01"}"#;

    fn instrument() -> Instrument {
        Instrument::new(
            Venue::Binance,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    fn bybit_instrument() -> Instrument {
        Instrument::new(
            Venue::Bybit,
            MarketType::Spot,
            Symbol::new("BTC/USDT").unwrap(),
        )
    }

    fn coinbase_instrument() -> Instrument {
        Instrument::new(
            Venue::Coinbase,
            MarketType::Spot,
            Symbol::new("BTC/USD").unwrap(),
        )
    }

    fn temp_directory(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("astra-normalize-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn record(seq: u64, channel: Channel, ts_socket: i64, payload: Vec<u8>) -> CaptureRecord {
        record_as(seq, &instrument(), channel, ts_socket, payload)
    }

    fn record_as(
        seq: u64,
        instrument: &Instrument,
        channel: Channel,
        ts_socket: i64,
        payload: Vec<u8>,
    ) -> CaptureRecord {
        CaptureRecord {
            seq,
            instrument: instrument.clone(),
            channel,
            ts_socket: Timestamp::from_unix_nanos(ts_socket),
            ts_exchange: None,
            payload,
            flags: CaptureFlags::NONE,
        }
    }

    fn write_capture(output: &Path, records: Vec<CaptureRecord>) {
        std::fs::create_dir_all(output.join(FRAMES_DIR)).unwrap();

        let mut writer = store::ChunkWriter::open(output.join(FRAMES_DIR), 100).unwrap();
        for record in &records {
            writer.append(record).unwrap();
        }
        writer.finish().unwrap();

        let venue_frames = records
            .iter()
            .filter(|record| !record.flags.contains(CaptureFlags::SYNTHETIC))
            .count() as u64;

        let manifest = CaptureManifest {
            schema_version: astra_types::SCHEMA_VERSION,
            capture_id: CaptureId::new("normalize-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument(),
            channel: Channel::BookDiff,
            frames_written: venue_frames,
            stop_reason: None,
        };
        astra_record::capture::write_manifest(output, &manifest).unwrap();
    }

    fn read_table(path: &Path) -> (Vec<RecordBatch>, Option<String>) {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = std::fs::File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
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
        let reader = builder.build().unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
        (batches, version)
    }

    fn rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[test]
    fn book_diff_capture_round_trips_through_parquet() {
        let input = temp_directory("round-trip");
        let output = temp_directory("round-trip-out");
        write_capture(
            &input,
            vec![
                record(
                    0,
                    Channel::BookDiff,
                    1_700_000_000_000_000_000,
                    DEPTH_FRAME.as_bytes().to_vec(),
                ),
                record(
                    1,
                    Channel::BookDiff,
                    1_700_000_000_100_000_000,
                    NEXT_FRAME.as_bytes().to_vec(),
                ),
            ],
        );

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.records, 2);
        assert_eq!(summary.rows_written, 2);
        assert_eq!(summary.files.len(), 1);

        let (batches, version) = read_table(&summary.files[0]);
        assert_eq!(version.as_deref(), Some("1"));
        assert_eq!(rows(&batches), 2);

        let venue = batches[0]
            .column_by_name("venue")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(venue.value(0), "binance");

        let bids = batches[0]
            .column_by_name("bids")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(bids.len(), 2);

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn venue_event_time_reaches_the_exchange_column() {
        // DEPTH_FRAME carries E=1700000000000 (millis); the normalized row
        // must carry it as nanoseconds instead of leaving ts_exchange null.
        let input = temp_directory("exchange-time");
        let output = temp_directory("exchange-time-out");
        write_capture(
            &input,
            vec![record(
                0,
                Channel::BookDiff,
                1_700_000_000_100_000_000,
                DEPTH_FRAME.as_bytes().to_vec(),
            )],
        );

        let summary = normalize(&input, &output).unwrap();
        let (batches, _) = read_table(&summary.files[0]);
        let ts_exchange = batches[0]
            .column_by_name("ts_exchange")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        assert!(ts_exchange.is_valid(0));
        assert_eq!(ts_exchange.value(0), 1_700_000_000_000_000_000);

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn decimal_values_survive_exactly() {
        let input = temp_directory("decimal");
        let output = temp_directory("decimal-out");
        write_capture(
            &input,
            vec![record(
                0,
                Channel::BookDiff,
                1_700_000_000_000_000_000,
                DEPTH_FRAME.as_bytes().to_vec(),
            )],
        );

        let summary = normalize(&input, &output).unwrap();
        let (batches, _) = read_table(&summary.files[0]);

        let bids = batches[0]
            .column_by_name("bids")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let values = bids.value(0);
        let values = values
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let prices = values
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(prices.value(0), 108105_12000000);

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_negative_quantity_rejects_the_whole_batch() {
        let input = temp_directory("reject");
        let output = temp_directory("reject-out");
        let bad = r#"{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":100,"u":105,"b":[["108105.12","-1"]],"a":[]}"#;
        write_capture(
            &input,
            vec![
                record(
                    0,
                    Channel::BookDiff,
                    1_700_000_000_000_000_000,
                    DEPTH_FRAME.as_bytes().to_vec(),
                ),
                record(
                    1,
                    Channel::BookDiff,
                    1_700_000_000_100_000_000,
                    bad.as_bytes().to_vec(),
                ),
            ],
        );

        assert!(matches!(
            normalize(&input, &output),
            Err(NormalizeError::Validation(_))
        ));
        assert!(parquet_files(&output).is_empty());

        std::fs::remove_dir_all(&input).unwrap();
        let _ = std::fs::remove_dir_all(&output);
    }

    #[test]
    fn gap_markers_become_synthetic_rows_with_reasons() {
        use astra_types::GapMarker;

        let input = temp_directory("gaps");
        let output = temp_directory("gaps-out");
        let marker = GapMarker {
            started_at: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
            ended_at: Timestamp::from_unix_nanos(1_700_000_000_100_000_000),
            attempts: 2,
            reason: "venue_close".to_owned(),
        };
        let mut gap = record(
            1,
            Channel::BookDiff,
            1_700_000_000_100_000_000,
            serde_json::to_vec(&marker).unwrap(),
        );
        gap.flags = CaptureFlags::SYNTHETIC
            .union(CaptureFlags::SEQUENCE_GAP)
            .union(CaptureFlags::UNRELIABLE);

        std::fs::create_dir_all(input.join(FRAMES_DIR)).unwrap();
        let mut writer = store::ChunkWriter::open(input.join(FRAMES_DIR), 100).unwrap();
        writer
            .append(&record(
                0,
                Channel::BookDiff,
                1_700_000_000_000_000_000,
                DEPTH_FRAME.as_bytes().to_vec(),
            ))
            .unwrap();
        writer.append(&gap).unwrap();
        writer.finish().unwrap();

        let manifest = CaptureManifest {
            schema_version: astra_types::SCHEMA_VERSION,
            capture_id: CaptureId::new("normalize-test"),
            created_at: Timestamp::from_unix_nanos(0),
            instrument: instrument(),
            channel: Channel::BookDiff,
            frames_written: 1,
            stop_reason: None,
        };
        astra_record::capture::write_manifest(&input, &manifest).unwrap();

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.rows_written, 2);

        let (batches, _) = read_table(&summary.files[0]);
        assert_eq!(rows(&batches), 2);

        let synthetic = batches[0]
            .column_by_name("synthetic")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(!synthetic.value(0));
        assert!(synthetic.value(1));

        let reasons = batches[0]
            .column_by_name("gap_reason")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert!(reasons.is_null(0));
        assert_eq!(reasons.value(1), "venue_close");

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn other_channels_are_counted_and_skipped() {
        let input = temp_directory("skipped");
        let output = temp_directory("skipped-out");
        write_capture(
            &input,
            vec![record(
                0,
                Channel::Funding,
                1_700_000_000_000_000_000,
                TRADE_FRAME.as_bytes().to_vec(),
            )],
        );

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.records, 1);
        assert_eq!(summary.rows_written, 0);
        assert_eq!(summary.skipped_unsupported_channel, 1);
        assert!(summary.files.is_empty());

        std::fs::remove_dir_all(&input).unwrap();
        let _ = std::fs::remove_dir_all(&output);
    }

    #[test]
    fn a_torn_manifest_rejects_normalization() {
        let input = temp_directory("torn");
        let output = temp_directory("torn-out");
        write_capture(
            &input,
            vec![record(
                0,
                Channel::BookDiff,
                1_700_000_000_000_000_000,
                DEPTH_FRAME.as_bytes().to_vec(),
            )],
        );

        let body = std::fs::read_to_string(input.join(MANIFEST_FILE)).unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(&body).unwrap();
        manifest["frames_written"] = serde_json::json!(99);
        std::fs::write(
            input.join(MANIFEST_FILE),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();

        assert!(matches!(
            normalize(&input, &output),
            Err(NormalizeError::Validation(_))
        ));

        std::fs::remove_dir_all(&input).unwrap();
        let _ = std::fs::remove_dir_all(&output);
    }

    #[test]
    fn normalizing_twice_yields_identical_bytes() {
        let input = temp_directory("deterministic");
        let first = temp_directory("deterministic-first");
        let second = temp_directory("deterministic-second");
        write_capture(
            &input,
            vec![
                record(
                    0,
                    Channel::BookDiff,
                    1_700_000_000_000_000_000,
                    DEPTH_FRAME.as_bytes().to_vec(),
                ),
                record(
                    1,
                    Channel::BookDiff,
                    1_700_000_000_100_000_000,
                    NEXT_FRAME.as_bytes().to_vec(),
                ),
            ],
        );

        let first_summary = normalize(&input, &first).unwrap();
        let second_summary = normalize(&input, &second).unwrap();

        assert_eq!(first_summary.files.len(), second_summary.files.len());

        let relative = |root: &Path, path: &Path| {
            path.strip_prefix(root)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        };
        let mut first_files: Vec<String> = first_summary
            .files
            .iter()
            .map(|path| relative(&first, path))
            .collect();
        let mut second_files: Vec<String> = second_summary
            .files
            .iter()
            .map(|path| relative(&second, path))
            .collect();
        first_files.sort();
        second_files.sort();
        assert_eq!(first_files, second_files);

        for name in &first_files {
            let a = std::fs::read(first.join(name)).unwrap();
            let b = std::fs::read(second.join(name)).unwrap();
            assert_eq!(a, b, "output differs between runs: {name}");
        }

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&first).unwrap();
        std::fs::remove_dir_all(&second).unwrap();
    }

    #[test]
    fn a_future_schema_version_is_refused_not_misread() {
        let input = temp_directory("schema-version");
        let output = temp_directory("schema-version-out");
        write_capture(
            &input,
            vec![record(
                0,
                Channel::BookDiff,
                1_700_000_000_000_000_000,
                DEPTH_FRAME.as_bytes().to_vec(),
            )],
        );

        let manifest_path = input.join(MANIFEST_FILE);
        let body = std::fs::read_to_string(&manifest_path).unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(&body).unwrap();
        manifest["schema_version"] = serde_json::json!(astra_types::SCHEMA_VERSION + 1);
        std::fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        assert!(matches!(
            normalize(&input, &output),
            Err(NormalizeError::SchemaVersion { .. })
        ));

        std::fs::remove_dir_all(&input).unwrap();
        let _ = std::fs::remove_dir_all(&output);
    }

    #[test]
    fn values_beyond_parquet_decimal_precision_are_rejected() {
        // Decimal128(20, 8) holds at most 10^20 - 1 unscaled. A hostile
        // 13-digit price parses as Fixed fine but does not fit the column;
        // the batch must be rejected whole, never written half-garbled.
        let input = temp_directory("decimal-range");
        let output = temp_directory("decimal-range-out");
        write_capture(
            &input,
            vec![record(
                0,
                Channel::BookDiff,
                1_700_000_000_000_000_000,
                br#"{"e":"depthUpdate","s":"BTCUSDT","U":100,"u":105,"b":[["1000000000000.00000000","1"]],"a":[]}"#.to_vec(),
            )],
        );

        assert!(matches!(
            normalize(&input, &output),
            Err(NormalizeError::Validation(_))
        ));

        std::fs::remove_dir_all(&input).unwrap();
        let _ = std::fs::remove_dir_all(&output);
    }

    #[test]
    fn an_empty_trade_bundle_yields_zero_rows_not_a_null_row() {
        // A transport message with zero prints holds zero market events, so
        // it normalizes to nothing — unlike garbage bytes, which keep a null
        // placeholder row to preserve seq accounting. Pinned, since replay
        // counts the same frame as skipped_unparseable (see failure policy).
        let input = temp_directory("empty-bundle");
        let output = temp_directory("empty-bundle-out");
        write_capture(
            &input,
            vec![record_as(
                0,
                &bybit_instrument(),
                Channel::Trade,
                1_700_000_000_000_000_000,
                br#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672304486868,"data":[]}"#.to_vec(),
            )],
        );

        let summary = normalize(&input, &output).unwrap();

        assert_eq!(summary.rows_written, 0);
        assert_eq!(summary.skipped_unsupported_channel, 0);
        assert!(parquet_files(&output).is_empty());

        std::fs::remove_dir_all(&input).unwrap();
        let _ = std::fs::remove_dir_all(&output);
    }

    #[test]
    fn decimal_precision_bound_is_exactly_ten_to_the_twenty() {
        // This constant was once written with 29 digits. Pin it: Decimal128
        // (20, 8) holds 10^20 - 1 unscaled, and the boundary price just below
        // must parse while just above must not fit.
        assert_eq!(DECIMAL_MAX_RAW, 10i128.pow(20) - 1);
        let just_fits: Fixed = "999999999999.99999999".parse().unwrap();
        assert_eq!(just_fits.raw(), DECIMAL_MAX_RAW);
        let too_big: Fixed = "1000000000000.00000000".parse().unwrap();
        assert!(too_big.raw() > DECIMAL_MAX_RAW);
    }

    #[test]
    fn rows_group_into_date_partitions() {
        let input = temp_directory("dates");
        let output = temp_directory("dates-out");
        write_capture(
            &input,
            vec![
                record(
                    0,
                    Channel::BookDiff,
                    1_700_000_000_000_000_000,
                    DEPTH_FRAME.as_bytes().to_vec(),
                ),
                record(
                    1,
                    Channel::BookDiff,
                    1_700_086_400_000_000_000,
                    NEXT_FRAME.as_bytes().to_vec(),
                ),
            ],
        );

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.files.len(), 2);

        let mut dates: Vec<String> = summary
            .files
            .iter()
            .map(|path| {
                path.components()
                    .rev()
                    .nth(1)
                    .unwrap()
                    .as_os_str()
                    .to_str()
                    .unwrap()
                    .to_owned()
            })
            .collect();
        dates.sort();
        assert_eq!(dates, vec!["date=2023-11-14", "date=2023-11-15"]);

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    const BINANCE_TRADE: &str = r#"{"e":"trade","E":1700000000000,"s":"BTCUSDT","t":6736601518,"p":"85976.95000000","q":"0.00148000","T":1791193512031,"m":false,"M":true}"#;
    const BYBIT_BUNDLE: &str = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672304486868,"data":[{"T":1672304486865,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"16578.50","i":"aaa","seq":1},{"T":1672304486866,"s":"BTCUSDT","S":"Sell","v":"0.002","p":"16578.51","i":"bbb","seq":2}]}"#;
    const COINBASE_MATCH: &str = r#"{"type":"match","sequence":136981933065,"trade_id":1100092822,"product_id":"BTC-USD","price":"83098.17000000","size":"0.00030386","side":"buy","time":"2026-09-29T17:37:59.857502Z","maker_order_id":"x","taker_order_id":"y"}"#;

    fn trade_file(output: &Path) -> PathBuf {
        parquet_files(output)
            .into_iter()
            .find(|path| {
                path.components().any(|component| {
                    component
                        .as_os_str()
                        .to_str()
                        .is_some_and(|s| s == "channel=trade")
                })
            })
            .unwrap()
    }

    fn decimal_column(batches: &[RecordBatch], name: &str) -> Vec<Option<i128>> {
        let mut values = Vec::new();
        for batch in batches {
            let column = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .unwrap();
            for index in 0..column.len() {
                if column.is_null(index) {
                    values.push(None);
                } else {
                    values.push(Some(column.value(index)));
                }
            }
        }
        values
    }

    fn string_column(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
        let mut values = Vec::new();
        for batch in batches {
            let column = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for index in 0..column.len() {
                if column.is_null(index) {
                    values.push(None);
                } else {
                    values.push(Some(column.value(index).to_owned()));
                }
            }
        }
        values
    }

    #[test]
    fn binance_trades_normalize_exactly() {
        let input = temp_directory("trades-bn");
        let output = temp_directory("trades-bn-out");
        write_capture(
            &input,
            vec![
                record(
                    0,
                    Channel::Trade,
                    1_700_000_000_000_000_000,
                    BINANCE_TRADE.as_bytes().to_vec(),
                ),
                record(
                    1,
                    Channel::Trade,
                    1_700_000_000_100_000_000,
                    BINANCE_TRADE.as_bytes().to_vec(),
                ),
            ],
        );

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.rows_written, 2);
        assert_eq!(summary.files.len(), 1);

        let (batches, version) = read_table(&summary.files[0]);
        assert_eq!(version.as_deref(), Some("1"));

        let prices = decimal_column(&batches, "price");
        assert_eq!(prices, vec![Some(85976_95000000), Some(85976_95000000)]);
        assert_eq!(
            string_column(&batches, "side"),
            vec![Some("Buy".to_owned()), Some("Buy".to_owned())]
        );
        assert_eq!(
            string_column(&batches, "trade_id"),
            vec![Some("6736601518".to_owned()), Some("6736601518".to_owned())]
        );

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn bybit_bundles_expand_to_indexed_rows() {
        let input = temp_directory("trades-bybit");
        let output = temp_directory("trades-bybit-out");
        write_capture(
            &input,
            vec![record_as(
                0,
                &bybit_instrument(),
                Channel::Trade,
                1_700_000_000_000_000_000,
                BYBIT_BUNDLE.as_bytes().to_vec(),
            )],
        );

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.rows_written, 2);

        let (batches, _) = read_table(&summary.files[0]);
        assert_eq!(
            string_column(&batches, "trade_id"),
            vec![Some("aaa".to_owned()), Some("bbb".to_owned())]
        );
        assert_eq!(
            string_column(&batches, "side"),
            vec![Some("Buy".to_owned()), Some("Sell".to_owned())]
        );

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn coinbase_matches_normalize_with_parsed_time() {
        let input = temp_directory("trades-cb");
        let output = temp_directory("trades-cb-out");
        write_capture(
            &input,
            vec![record_as(
                0,
                &coinbase_instrument(),
                Channel::Trade,
                1_700_000_000_000_000_000,
                COINBASE_MATCH.as_bytes().to_vec(),
            )],
        );

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.rows_written, 1);

        let (batches, _) = read_table(&summary.files[0]);
        assert_eq!(
            string_column(&batches, "side"),
            vec![Some("Buy".to_owned())]
        );

        use arrow::array::Array;
        let ts: Vec<Option<i64>> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("ts_exchange")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                (0..column.len())
                    .map(|index| (!column.is_null(index)).then(|| column.value(index)))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ts.len(), 1);
        assert!(ts[0].is_some());

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn mixed_channels_write_separate_tables() {
        let input = temp_directory("mixed");
        let output = temp_directory("mixed-out");
        write_capture(
            &input,
            vec![
                record(
                    0,
                    Channel::BookDiff,
                    1_700_000_000_000_000_000,
                    DEPTH_FRAME.as_bytes().to_vec(),
                ),
                record(
                    1,
                    Channel::Trade,
                    1_700_000_000_100_000_000,
                    BINANCE_TRADE.as_bytes().to_vec(),
                ),
            ],
        );

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.rows_written, 2);
        assert_eq!(summary.files.len(), 2);
        assert!(
            trade_file(&output)
                .to_str()
                .unwrap()
                .contains("channel=trade")
        );

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_bad_trade_rejects_its_table_only() {
        let input = temp_directory("bad-trade");
        let output = temp_directory("bad-trade-out");
        let bad = br#"{"e":"trade","E":1700000000000,"s":"BTCUSDT","t":9,"p":"-5","q":"0.01","T":1791193512031,"m":false,"M":true}"#;
        write_capture(
            &input,
            vec![
                record(
                    0,
                    Channel::BookDiff,
                    1_700_000_000_000_000_000,
                    DEPTH_FRAME.as_bytes().to_vec(),
                ),
                record(1, Channel::Trade, 1_700_000_000_100_000_000, bad.to_vec()),
            ],
        );

        assert!(matches!(
            normalize(&input, &output),
            Err(NormalizeError::Validation(_))
        ));

        std::fs::remove_dir_all(&input).unwrap();
        let _ = std::fs::remove_dir_all(&output);
    }

    const BINANCE_TICKER: &str = r#"{"u":101057329065,"s":"BTCUSDT","b":"86086.00000000","B":"8.17272000","a":"86086.01000000","A":"0.02862000"}"#;
    const COINBASE_TICKER: &str = r#"{"type":"ticker","sequence":136981932897,"product_id":"BTC-USD","price":"83098.17","best_bid":"83098.17000000","best_bid_size":"0.05203270","best_ask":"83098.18000000","best_ask_size":"0.00119371","time":"2026-09-29T17:37:59.446731Z"}"#;

    fn top_book_file(output: &Path) -> PathBuf {
        parquet_files(output)
            .into_iter()
            .find(|path| {
                path.components().any(|component| {
                    component
                        .as_os_str()
                        .to_str()
                        .is_some_and(|s| s == "channel=book_ticker")
                })
            })
            .unwrap()
    }

    #[test]
    fn binance_tickers_normalize_exactly() {
        let input = temp_directory("top-bn");
        let output = temp_directory("top-bn-out");
        write_capture(
            &input,
            vec![
                record(
                    0,
                    Channel::BookTicker,
                    1_700_000_000_000_000_000,
                    BINANCE_TICKER.as_bytes().to_vec(),
                ),
                record(
                    1,
                    Channel::BookTicker,
                    1_700_000_000_100_000_000,
                    BINANCE_TICKER.as_bytes().to_vec(),
                ),
            ],
        );

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.rows_written, 2);

        let (batches, version) = read_table(&top_book_file(&output));
        assert_eq!(version.as_deref(), Some("1"));
        assert_eq!(
            decimal_column(&batches, "best_bid"),
            vec![Some(86086_00000000), Some(86086_00000000)]
        );
        assert_eq!(
            decimal_column(&batches, "best_ask_qty"),
            vec![Some(2862000), Some(2862000)]
        );

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn coinbase_tickers_normalize_with_exchange_time() {
        let input = temp_directory("top-cb");
        let output = temp_directory("top-cb-out");
        write_capture(
            &input,
            vec![record_as(
                0,
                &coinbase_instrument(),
                Channel::BookTicker,
                1_700_000_000_000_000_000,
                COINBASE_TICKER.as_bytes().to_vec(),
            )],
        );

        let summary = normalize(&input, &output).unwrap();
        assert_eq!(summary.rows_written, 1);

        let (batches, _) = read_table(&top_book_file(&output));
        assert_eq!(
            decimal_column(&batches, "best_bid"),
            vec![Some(83098_17000000)]
        );
        assert_eq!(
            string_column(&batches, "venue"),
            vec![Some("coinbase".to_owned())]
        );

        use arrow::array::Array;
        let batch = &batches[0];
        let ts = batch
            .column_by_name("ts_exchange")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(!ts.is_null(0));

        std::fs::remove_dir_all(&input).unwrap();
        std::fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn a_bad_top_rejects_its_table_only() {
        let input = temp_directory("bad-top");
        let output = temp_directory("bad-top-out");
        let bad = br#"{"u":1,"s":"BTCUSDT","b":"-5","B":"1","a":"1","A":"1"}"#;
        write_capture(
            &input,
            vec![
                record(
                    0,
                    Channel::BookDiff,
                    1_700_000_000_000_000_000,
                    DEPTH_FRAME.as_bytes().to_vec(),
                ),
                record(
                    1,
                    Channel::BookTicker,
                    1_700_000_000_100_000_000,
                    bad.to_vec(),
                ),
            ],
        );

        assert!(matches!(
            normalize(&input, &output),
            Err(NormalizeError::Validation(_))
        ));

        std::fs::remove_dir_all(&input).unwrap();
        let _ = std::fs::remove_dir_all(&output);
    }
}
