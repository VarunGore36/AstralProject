# Astral Project

**An open, reproducible measurement layer for crypto markets.**

Astra makes it possible to determine whether a trading idea actually survives
fees, slippage, latency, liquidity and realistic fills — and to reproduce that
determination later.

Historical research and simulation only. No execution, no trading, no capital.
A rigorous "this has no edge" is a successful result here.

## Status

- **Done** — core types, chunked capture store with SHA-256 integrity index,
  `astra-record init`, `astra-record capture` verified against the live Binance
  book-diff feed, reconnect with explicit gap records, venue update-ID
  continuity checking, L2 order-book reconstruction from captured frames,
  snapshot bootstrap verified against a live venue snapshot.
- **Working on** — top-of-book comparison against an independent venue
  reference, a second venue, additional channels.
- **Next (one thing)** — compare the reconstructed book against a
  venue-published reference taken at the same update ID, to close the Gate 1
  correctness claim.

## What exists today

| Component | State |
| --- | --- |
| Workspace, build, tests, lint | DONE |
| `Fixed` fixed-point decimal | DONE |
| `Timestamp` nanosecond clock value | DONE |
| Venue, market type, symbol, channel identifiers | DONE |
| `CaptureRecord`, `CaptureManifest`, `CaptureFlags`, `GapMarker` | DONE |
| `astra-record init` capture layout | DONE |
| Chunked record store with SHA-256 integrity index | DONE |
| Live Binance capture, `book_diff` channel | DONE |
| Reconnect with explicit gap records | DONE |
| Venue update-ID continuity checking | DONE |
| Bybit feed | NOT IMPLEMENTED |
| Channels other than `book_diff` | NOT IMPLEMENTED |
| Order-book state and level updates | DONE |
| Reconstruction from captured frames | DONE |
| Snapshot bootstrap for a complete book | DONE |
| Bootstrap verified against a live venue snapshot | DONE |
| Top-of-book match against an independent venue reference | NOT VERIFIED |
| Exchange checksum validation | NOT IMPLEMENTED |
| Normalised Parquet datasets | NOT IMPLEMENTED |
| Deterministic replay | NOT IMPLEMENTED |
| Cost and execution model | NOT IMPLEMENTED |

Nothing above is a stub dressed up as finished. The gaps are the roadmap.

## Data flow

```mermaid
flowchart LR
    A[Exchange WebSocket] --> B[raw immutable frames]
    B --> C[compressed chunks]
    C --> D[normalised Parquet]
    D --> E[deterministic replay]
    E --> F[research results]
```

| Stage | State |
| --- | --- |
| Exchange WebSocket | PARTIALLY IMPLEMENTED |
| Raw immutable frames | PARTIALLY IMPLEMENTED |
| Compressed chunks | DONE |
| Normalised Parquet | NOT IMPLEMENTED |
| Deterministic replay | NOT IMPLEMENTED |
| Research results | NOT IMPLEMENTED |

Partially implemented means one venue and one channel. Binance `book_diff` is
connected and verified; Bybit and every other channel are not connected yet.

## Repository layout

| Path | Purpose |
| --- | --- |
| `crates/astra-types` | The schema: decimal and timestamp primitives, identifiers, capture records |
| `crates/astra-book` | Order-book state: level updates, top of book, invariants |
| `crates/astra-record` | Lossless market-data capture and reconstruction from captures |
| `ROADMAP.md` | Gates with measurable definitions of done |

## Data model

| Type | Representation | Rule |
| --- | --- | --- |
| `Fixed` | `i128` scaled by 10^8 | decimal strings in, canonical eight places out; more than eight places is rejected rather than silently rounded; arithmetic rounds half away from zero |
| `Timestamp` | `i64` nanoseconds since epoch | ordered, serialised as raw nanoseconds |
| `Symbol` | validated `BASE/QUOTE` | uppercase ASCII alphanumeric with `.`, `_`, `-` |
| `Venue` | `binance`, `bybit` | parsed case-insensitively, stored canonically |
| `MarketType` | `spot`, `perp_usdt` | |
| `Channel` | `book_diff`, `book_snapshot`, `trade`, `book_ticker`, `funding`, `open_interest`, `liquidation` | |

Floating point is for derived analytics only, through
`to_f64_for_analytics`. Prices, quantities, fees and PnL never touch it.

## Capture records

| Field | Meaning |
| --- | --- |
| `seq` | position in the capture stream |
| `instrument` | venue + market type + symbol |
| `channel` | kind of market data |
| `ts_socket` | when the frame was read locally |
| `ts_exchange` | timestamp supplied by the venue, when present |
| `payload` | raw bytes, untouched |
| `flags` | capture quality bits |

| Flag | Meaning |
| --- | --- |
| `SEQUENCE_GAP` | the venue stream skipped a sequence number |
| `DUPLICATE` | the frame repeats one already recorded |
| `RESYNC` | the capture resynchronised after a gap |
| `STALE` | the data was too old to trust at capture time |
| `UNRELIABLE` | the surrounding window cannot be trusted |
| `TRUNCATED` | the payload was cut short |
| `SYNTHETIC` | the record was written by the recorder, not by the venue |

## Gap records

Two things produce a gap record: a feed that dropped and was re-established, and
a discontinuity in the venue's own update sequence. Both write one synthetic
record into the stream rather than letting the hole go unnoticed. It carries
`SYNTHETIC | SEQUENCE_GAP | UNRELIABLE` and a JSON payload:

```json
{ "started_at": 1790420019514000000,
  "ended_at": 1790420021770000000,
  "attempts": 1,
  "reason": "venue_close" }
```

`attempts` is the reconnect attempt count and is 0 for sequence gaps. `reason`
is `venue_close`, `read_error: ...` or `update_id_gap: expected N, saw M`.

A reader that filters out `SYNTHETIC` records sees only venue data; a reader
that does not will find the gap explicitly marked instead of silently
absorbing it. `frames_written` in the manifest counts venue frames only.

## Continuity checking

For Binance `book_diff` every event carries `U`, the first update id, and `u`,
the last. A well-formed stream has each event's `U` equal to the previous
event's `u` + 1. The recorder checks exactly that and writes the gap record
before the frame that breaks the rule.

Continuity is only checked where the payload format is understood. Frames that
cannot be parsed are not checked, and the run reports `checked` alongside
`frames` so that a zero sequence-gap count is only meaningful when the two
numbers match.

## Order book

Reconstruction applies venue level updates to a two-sided book. A quantity of
zero removes the level; any other quantity sets it. Prices and quantities are
fixed-point decimals throughout, so no float rounding can move a level.

Without a snapshot the book is **partial**: levels never touched by an update
are absent. With one it is complete:

```sh
cargo run -p astra-record -- reconstruct --input ./capture --snapshot ./snapshot.json
```

The snapshot is a venue depth snapshot (`lastUpdateId` plus bids and asks).
Events ending at or before the snapshot id are skipped, the first overlapping
event applies, and every later event must continue the update-id sequence or
the book is marked broken. A broken book rejects all further events until a
new snapshot arrives; it never silently resumes.

```sh
cargo run -p astra-record -- reconstruct --input ./capture
```

Reports how many frames were applied, skipped, or rejected, how many could not
be parsed, the level counts, top of book, mid, spread, and whether the book
ever crossed. A crossed book is a reconstruction error, and the run says so
rather than presenting the numbers anyway.

## Capture layout

```text
capture/
├── manifest.json
└── frames/
    ├── index.json
    ├── chunk-000000.zst
    ├── chunk-000001.zst
    └── ...
```

`manifest.json` records the schema version, a generated capture id, the
creation time, the instrument, the channel, the number of frames written and
the reason capture stopped. A capture that ended because the venue dropped the
connection says so; it never looks like a clean finish.

`index.json` records, for every chunk: its name, the first and last sequence
number it holds, the record count, the byte size and the SHA-256 of the
compressed file. Reading a capture verifies every chunk against that hash
before decoding it, so silent corruption is detected rather than absorbed.

## Chunk format

A chunk is a zstd-compressed stream of length-prefixed records: a four byte
little-endian length followed by the encoded record. Payload bytes are stored
exactly as received and are never re-encoded, truncated or interpreted.

Chunks roll after a fixed number of records. A writer reopens an existing
capture and continues the chunk sequence rather than overwriting it.

## Quickstart

```sh
cargo build --workspace
cargo test --workspace

cargo run -p astra-record -- init \
  --output ./capture \
  --venue binance \
  --market spot \
  --symbol BTC/USDT \
  --channel book_diff
```

Capture from a live feed:

```sh
cargo run -p astra-record -- capture \
  --output ./capture \
  --venue binance \
  --market spot \
  --symbol BTC/USDT \
  --channel book_diff \
  --duration-secs 3600
```

The run stops at the end of the duration, at `--max-frames`, on Ctrl-C, or when
the feed cannot be kept alive, and records which of those happened in the
manifest. `--url` overrides the derived feed for local testing.

A dropped connection is re-established up to `--max-reconnects` times (default
5) with exponential backoff, and every reconnection writes a gap record. Pass
`--max-reconnects 0` to stop at the first drop instead.

## Feeds

| Venue | Market | Channel | Stream |
| --- | --- | --- | --- |
| binance | spot | book_diff | `wss://stream.binance.com:9443/ws/<symbol>@depth@100ms` |
| binance | perp_usdt | book_diff | `wss://fstream.binance.com/ws/<symbol>@depth@100ms` |

Every other combination is refused with an explicit not-implemented error rather
than silently falling back to something else.

## Known limitations

- `ts_exchange` is not populated at capture time. Reading the venue timestamp
  out of the payload is normalisation work and happens later.
- Ctrl-C is handled, but a hard kill loses the chunk currently in memory. The
  capture manifest and every closed chunk survive; the partial one does not.
- One venue and one channel are connected: Binance `book_diff`. Bybit and every
  other channel are not implemented, so update-ID continuity checking has no
  rule defined for them yet.
- Continuity checking assumes the venue stream is strictly sequential. A venue
  that coalesces or reorders updates would produce false gaps; no such case has
  been observed on the data captured so far.
- The reconstructed book is **complete only with a snapshot**. Without one,
  levels never touched by an update are absent and top-of-book is indicative
  rather than authoritative.
- The venue REST endpoint is intermittently unreachable from the build
  environment (TLS interception with `UnknownIssuer` on `api.binance.com`).
  Snapshot and stream validation currently runs through the official public
  mirrors `data-api.binance.vision` and `data-stream.binance.vision` with a
  `--url` override, and the README says so instead of pretending otherwise.

## Verification record

What has actually been checked, and what has not. Nothing here is inferred from
the fact that the code compiles.

| Claim | Evidence | Status |
| --- | --- | --- |
| Value types round-trip exactly | unit tests, `cargo test --workspace` | VERIFIED |
| Chunk store round-trips records byte for byte | unit tests | VERIFIED |
| Chunk store detects a corrupted chunk | test flips one byte and expects an integrity error | VERIFIED |
| Integrity hashes are truthful | recorded SHA-256 cross-checked against the system `sha256sum` | VERIFIED |
| Capture writes frames from a real WebSocket | end-to-end run: handshake, frames, manifest and index on disk | VERIFIED |
| Live Binance capture | 60 second run: 601 frames at the expected 10/s rate, payloads are `depthUpdate` events, chunk hash matches the system `sha256sum` | VERIFIED |
| Reconnect writes gap records and continues | unit tests drop the feed mid-capture and check the marker, its span and the continued sequence | VERIFIED |
| Update-ID continuity checking | parser tested against a real captured Binance frame; live run checked 601 of 601 frames and reported 0 gaps; unit tests inject a discontinuity and confirm it is detected and marked | VERIFIED |
| Feed URLs for Binance spot and perp | unit tests | VERIFIED |
| Order-book level updates, including removals | unit tests plus a real captured frame that contains two zero-quantity removals | VERIFIED |
| Reconstruction from captured frames | 601-frame live capture: all 601 applied, 0 unchecked, 0 invalid, 280 bid and 258 ask levels, spread of one tick, book never crossed | VERIFIED |
| Snapshot bootstrap against a live venue snapshot | 400-frame capture with a mid-stream snapshot: 133 pre-snapshot events skipped (matches an independent count), 267 applied, 0 gaps, 0 rejected, overlap event at exactly S+1, spread of one tick, book never crossed | VERIFIED |
| Losslessness over a long soak | none | NOT VERIFIED |
| Top-of-book match against an independent venue reference | none — the end snapshot was 5,000 updates past the last captured event, so a direct comparison would measure market movement rather than reconstruction error | NOT VERIFIED |
| Exchange checksum validation | none | NOT IMPLEMENTED |

## Failures encountered

Recorded rather than tidied away.

**The TLS path was completely broken.** rustls panicked on the first secure
connection because no crypto provider was installed — `tungstenite` enables
rustls without a backend, so every `wss://` connection would have crashed on
startup. The test suite did not catch it, because the test server speaks plain
`ws://` and never touches the TLS stack. It surfaced on the first attempt to run
against a real feed. Fixed by enabling the `ring` backend and installing the
provider explicitly.

**A test failed because its fixture was wrong.** The duration-limit test used a
server that closed the connection after 50 ms, so the close beat the limit and
the capture reported the wrong stop reason. The capture loop was correct; the
harness was not.

**A gap counter counted the wrong kind of gap.** The first continuity checker
incremented the connection-gap counter from the shared gap-writing function, so
a sequence gap was tallied as a connection gap. The test failed on the count
rather than on the detection, which is what tests are for. Fixed by removing
the counter from the writer and letting the caller classify each record.

**A test asserted a belief that the real data contradicted.** The first
order-book tests expected eight bid levels from a real captured frame. The
frame actually holds six: two of its quantities are zero, which means remove,
not add. The fixture corrected the test, not the other way round — which is the
entire reason the fixture is a real frame rather than invented JSON.

**The event iterator ate the event it stopped on.** The reference comparison
walked events with `for ... in pending.by_ref()` and `break` when an event
passed the reference snapshot. `break` consumes the current item, so that event
was silently lost from the next comparison window. Three tests failed on counts
before the cause was found. Fixed with an index-based walk that only advances
past consumed events.

**A repaired book stayed marked broken.** `is_reliable()` combined a state flag
with a cumulative gap counter, so loading a fresh snapshot after a gap left the
book permanently unreliable. The counter is history and the flag is state; only
the flag belongs in the predicate.

**The snapshot boundary was off by one.** Events ending exactly at the snapshot
id were applied instead of skipped, re-applying updates the snapshot already
contains. Live validation caught it: the independent count said 133 pre-snapshot
events and the recorder said 132. Fixed to skip events with `last <= snapshot`
as the venue protocol requires, with a regression test on the boundary.

**Live Binance capture failed before it succeeded.** The first attempt died
with `invalid peer certificate: UnknownIssuer`. Investigation showed the
network was intercepting TLS to `binance.com` with a Fortinet firewall: the
certificate presented for `*.binance.com` was issued by `Fortinet; Certificate
Authority` rather than a public authority, and the REST endpoint answered 403.
rustls was right to refuse it.

A later retry succeeded — the interception was gone, both endpoints presented
valid DigiCert certificates, and the capture ran normally. So that was a
transient network condition, not a property of the code or of the venue. It is
recorded because the first result was a real failure and the diagnosis is worth
keeping: if `UnknownIssuer` reappears, something on that network is doing TLS
inspection, and it will break any rustls or Go client rather than this one
specifically.

## Working rules

- Fixed-point integers for all financial arithmetic.
- Never silently drop data. Lossy paths emit explicit gap events.
- Determinism first: replaying a captured period must be byte identical.
- Measure before optimising. Every optimisation needs a benchmark.
- Anything touching prices, quantities, fees, PnL or fills requires tests.
- A stub is labelled NOT IMPLEMENTED, SIMULATED or MOCKED. Never reported as done.
- Failures, partial results and unverified claims are recorded in this README,
  not hidden. A gap is stated as a gap.

## What this is not

Not a trading bot. Not a signal service. Not a claim about returns. It is
infrastructure for deciding whether a claim about markets survives contact with
realistic costs.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
