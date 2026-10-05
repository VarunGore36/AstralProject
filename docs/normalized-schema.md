# Normalized event schema (v1)

Status: **implemented for `book_diff`, `trade`, and `top_of_book`**
(`astra-normalize`). `funding` and `liquidation` remain reserved table names
only. The spec below is the format the normalizer targets and the replay engine
builds against — one decided format instead of three invented ones.

## Why this document comes before code

Every downstream claim depends on this schema: deterministic replay needs
byte-identical normalized output, the audit trail needs provenance, and
research needs queryable columns. Designing it now forces the decisions while
they are still cheap. Changing it later means migrating stored data.

## Sources

The schema is derived from what the recorder already captures, not imagined:

- `CaptureRecord`: `seq`, instrument (venue + market type + symbol), channel,
  `ts_socket`, optional `ts_exchange`, raw `payload`, `CaptureFlags`
- Binance `depthUpdate`: `U`/`u` update ids, `b`/`a` levels, event time `E`
- Bybit `orderbook`: `u` version, `b`/`a` levels, `type` snapshot/delta
- Bybit `publicTrade`, `allLiquidation`; Coinbase `matches`, `ticker`
- `GapMarker`: synthetic records marking holes with reasons

Anything the current capture cannot produce is marked as such below rather
than specified wishfully.

## Tables

One table per channel family. A single unified table would force nullable
columns for everything and make every query pay for every channel's shape.

### `book_diff`

One row per venue book-update event.

| Column | Type | Meaning |
| --- | --- | --- |
| `venue` | string | `binance`, `bybit` (more when connected) |
| `market_type` | string | `spot`, `perp_usdt` |
| `symbol` | string | canonical `BASE/QUOTE` (`BTC/USDT`, `BTC/USD`) |
| `ts_exchange` | int64 ns, nullable | venue event time when the payload carries one (`E` on Binance depth) |
| `ts_socket` | int64 ns | local read time, always present |
| `ts_ready` | int64 ns | when the normalized row was produced — **not captured yet** |
| `first_update_id` | uint64, nullable | Binance `U`; Bybit `u` |
| `last_update_id` | uint64, nullable | Binance `u`; Bybit `u` |
| `bids` | list of (price, qty) fixed decimal | levels touched by this event |
| `asks` | list of (price, qty) fixed decimal | levels touched by this event |
| `capture_id` | string | source capture |
| `seq` | uint64 | position in the source capture |
| `flags` | uint32 | capture quality bits |
| `synthetic` | bool | true for gap markers (which carry no levels) |
| `gap_reason` | string, nullable | marker reason (`venue_close`, `read_error: …`, `update_id_gap: …`) |
| `gap_attempts` | uint32, nullable | reconnect attempts (0 for sequence gaps) |
| `gap_started` | int64 ns, nullable | last good frame before the hole |
| `gap_ended` | int64 ns, nullable | first frame after the hole |

### `trade`

One row per venue trade print. Aggregated messages (Bybit `publicTrade`
bundles) expand to one row per trade, distinguished by `print_index` —
decided because a bundle is a transport artifact, not a market event.

| Column | Type | Meaning |
| --- | --- | --- |
| `venue`, `market_type`, `symbol` | strings | as above |
| `ts_exchange` | int64 ns, nullable | trade time when carried and parseable (`T` millis; Coinbase RFC3339, nulled when unparseable) |
| `ts_socket` | int64 ns | local read time |
| `ts_ready` | int64 ns | **not captured yet** |
| `trade_id` | string, nullable | venue trade id as sent (numbers stringified, strings preserved) |
| `price`, `quantity` | fixed decimal, nullable | null only on synthetic gap rows |
| `side` | string, nullable | canonical `Buy`/`Sell`; derived from the maker flag on Binance, passed through on Bybit, case-normalized on Coinbase |
| `print_index` | uint32 | 0-based position within the source message; always 0 except expanded bundles |
| `capture_id`, `seq`, `flags`, `synthetic` | | as above |
| `gap_reason`, `gap_attempts`, `gap_started`, `gap_ended` | nullable | as in `book_diff`; gap markers arrive per channel |

### `top_of_book`

One row per top-of-book update (Binance `bookTicker`, Coinbase `ticker`).

| Column | Type | Meaning |
| --- | --- | --- |
| `venue`, `market_type`, `symbol` | strings | as above |
| `ts_exchange`, `ts_socket`, `ts_ready` | int64 ns | `ts_ready` **not captured yet** |
| `best_bid`, `best_bid_qty` | fixed decimal, nullable | null only on synthetic gap rows |
| `best_ask`, `best_ask_qty` | fixed decimal, nullable | null only on synthetic gap rows |
| `capture_id`, `seq`, `flags`, `synthetic` | | as above |
| `gap_reason`, `gap_attempts`, `gap_started`, `gap_ended` | nullable | as in `book_diff`; gap markers arrive per channel |

### `funding` and `liquidation`

Specified only as reserved table names. No parser exists, and Binance
`funding`/`liquidation` are not live-verified (geo-blocked). Columns will be
defined from the first real payloads, not from documentation.

## Fixed rules

- Prices and quantities are fixed-point decimals (scale 10^8, matching
  `Fixed`). No floats in any table, for the same reason as in code.
- Timestamps are int64 nanoseconds since epoch. The three-clock system
  (`exchange`, `socket`, `ready`) is what makes latency measurable later.
- Raw payloads are **not** stored in normalized tables. The raw chunks remain
  the source of truth; normalization must be re-derivable from them or it is
  not a normalization.
- Synthetic gap markers become rows with `synthetic = true` and null
  market fields, so a replay over normalized data sees the same holes as a
  replay over raw data.

## Partitioning and files

```text
dataset/
└── venue=binance/
    └── market_type=spot/
        └── symbol=BTC_USDT/
            └── channel=book_diff/
                └── date=2026-09-30/
                    ├── part-00000.parquet
                    └── ...
```

Hive-style partitions (`venue`, `market_type`, `symbol`, `channel`, `date`).
Symbols use `_` instead of `/` because `/` is a path separator. One writer per
partition per day; files are immutable once closed, mirroring chunk semantics.

## Validation rules

Every normalized batch must satisfy these before it is accepted:

```text
timestamps within [capture start, capture end + tolerance]   [deferred: tolerance undecided]
prices > 0 where present                                     [enforced]
quantities >= 0 where present                                [enforced]
update ids continuous within the batch unless a synthetic gap row intervenes   [not enforced: continuity is the capture layer's job, checked there]
no duplicate (capture_id, seq, print_index) triples          [enforced]
schema version recorded in file metadata, equal to the writer's version   [enforced]
```

A batch that fails validation is rejected whole, never partially accepted.

## Open questions (undecided, deliberately)

- The tolerance on timestamp validation (clock skew between venue and local).
- Whether `ts_ready` belongs in the table or in sidecar metadata.
- Compression codec and row-group sizing (measure on real data first).
- How normalization itself is made deterministic (sorting, float-free
  conversions, no wall-clock reads) — required before the determinism claim,
  designed with the replay engine, not here.

## What this document is not

Not a migration plan, not a query engine, not a promise of Parquet files by
any date. It is the format that future code must target, so that when the
normalizer is built, its output is already specified.
