# Replay engine design (v1)

Status: **implemented** (`astra-replay`: event core, `book-top` and
`trade-tally` strategies, CLI, chunk-streamed reading with `seq`
enforcement, plus `replay_normalized` over Parquet trees).
This document is the contract the engine satisfies — written before the engine
so the engine can be judged against it. The v1 scope below has since grown to
cover `trade` and `top_of_book` alongside `book_diff`; the rules are unchanged.

## What replay is

Deterministic re-emission of captured events, in order, driving strategy
callbacks. Given the same capture, the same code, and the same seed, every
replay produces identical signals. Replay answers: "what would this logic
have seen, had it been running during the capture?"

Replay is **not** a backtester. It performs no fills, holds no positions,
models no costs. Execution simulation is a separate engine that consumes
replay output. Conflating the two is how look-ahead bias and fill fantasy
enter through the back door.

## Input: raw captures first, Parquet second

Replay reads raw capture directories (chunks + index + manifest), streamed
chunk-by-chunk with `seq` continuity enforced across chunk boundaries —
not normalized datasets. Reasons:

- The raw capture is the source of truth; replaying a derived view cannot
  validate the derivation.
- The determinism claim itself is "replays of one captured day produce byte
  identical normalised output" — replay must therefore be able to *feed*
  normalization, which requires raw input.
- Gap markers, flags, and unparseable frames exist in raw form. A replay that
  cannot surface them is a cleaner fiction than the capture, not a replay.

Since v1, `replay_normalized` also replays Parquet trees — as an
equivalence prover, not a validator. It exists to prove one answer to the
null-row question (null rows replay as counted skips, with byte-identical
signal hashes to raw replay), and its scale limits are stated on the
function: O(events) memory, while raw replay streams. The three reasons
above are why raw stays the scale path and the derivation stays checkable.

## Event model (v1 scope)

Replays emit, in order:

- `book_diff` events with their update spans and level sets
- `trade` events, one per print (Bybit bundles expand via `print_index`)
- `top_of_book` events (Binance `bookTicker`, Coinbase `ticker`)
- synthetic gap markers, exactly as recorded
- stream end

A replay over a capture containing other channels replays the subset above
and reports the rest as skipped — the same accounting as the normalizer.

## Ordering rules

Single capture: `seq` order, which is capture order. No reordering, ever.

Multiple captures (v2, not v1): `ts_socket` order with `(capture_id, seq)`
as tiebreak. Timestamp ties across captures are real (two venues, same
millisecond) and the tiebreak must be total and stable, or two runs diverge.

## Virtual time

Strategies observe event timestamps, never the wall clock. The engine exposes
a virtual clock pinned to the current event's `ts_socket`. Any strategy code
path that reads wall-clock time, spawns threads, iterates hash maps, or draws
unseeded randomness breaks determinism by construction, and the engine cannot
detect it — so the strategy interface must make the deterministic path the
easy one (seeded RNG handle provided, no clock provided).

## Strategy interface

```rust
trait Strategy {
    fn on_book_diff(&mut self, event: &BookDiffEvent, ctx: &mut Context);
    fn on_snapshot(&mut self, event: &SnapshotEvent, ctx: &mut Context);
    fn on_trade(&mut self, event: &TradeEvent, ctx: &mut Context);
    fn on_top_of_book(&mut self, event: &TopBookEvent, ctx: &mut Context);
    fn on_gap(&mut self, marker: &GapMarker, ctx: &mut Context);
    fn on_end(&mut self, frames: u64, ctx: &mut Context);
}
```

`on_trade` defaults to ignoring (book-only strategies), as does
`on_top_of_book`; the rest are required. `TradeEvent` carries `print_index`
for bundle expansion; `Context` carries virtual time, the seeded RNG, and a
signal sink. Signals are opaque bytes plus a schema tag — the engine hashes
them per run, and equal hashes across runs is the determinism test. The
engine never interprets a signal; interpretation is the backtester's job.

## Gap semantics

Gaps surface as events, exactly as they occurred live. The strategy sees the
same hole the live system would have seen, and any book the strategy keeps
must break the same way the reconstructor breaks. A replay that papers over
gaps measures a market that never existed.

## Determinism rules

- Same capture bytes + same code + same seed → identical signal hashes.
- No wall-clock reads in the event path (enforced by review, not by compiler).
- No hash-map iteration anywhere a signal depends on order; `BTreeMap` where
  order matters.
- Single-threaded event loop for v1. Parallelism, if ever added, merges
  deterministically or not at all.
- `ts_ready` (normalization time) is excluded from signal-relevant state —
  it differs per run by definition.

## Acceptance criteria

- Ten replays of one captured day → ten identical signal hashes (Gate 1 bar).
  Performed on a 151-frame live capture; the CI determinism gate
  (`ops/determinism.sh`) now enforces the same property on a pinned fixture
  on every push.
- Same capture + same code on a second machine → identical hashes
  (cross-machine bar; needs a second machine to execute, mechanism defined here).
- A replay with an injected gap produces the documented broken-book behavior,
  not a silent continuation (covered by strategy tests; gaps break the book
  until a snapshot heals it).

## Open questions (undecided, deliberately)

- As-fast-as-possible only for v1, or a paced mode for debugging?
- Multi-instrument merge semantics (v2 ordering rule is sketched above but
  untested).

## Decided since v1 (was open, now closed)

- `trade` / `top_of_book` event shapes: implemented (`TradeEvent` with
  `print_index`, `TopBookEvent`).
- CLI surface: the engine ships as its own binary (`astra-replay`), not a
  `astra-record` subcommand.
- Signal recording and hashing: the harness registry records reports plus
  configs with reproduction verdicts (`verify`).
- Scale: raw replay streams chunk-by-chunk (O(chunk) inputs, `seq`
  enforced); normalized replay exists as the equivalence prover with
  stated O(events) limits.

## What this document is not

Not an implementation plan with dates, not a backtester design, not a
strategy SDK. It is the contract the future engine must satisfy, written
before the engine so the engine can be judged against it.
