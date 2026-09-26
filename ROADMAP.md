# Roadmap

## Gate 1 — capture and reconstruct

Build a small system that can record live market data losslessly and prove its
own book reconstruction is correct.

| Claim | Measurable done | State |
|---|---|---|
| Capture is lossless | zero dropped raw frames over a 72 hour soak across all instruments, verified by websocket sequence continuity and a file integrity manifest | OPEN — 60 s runs clean, no soak yet |
| Book reconstruction is correct | local book matches venue published checksum on at least 99.99% of checksum frames, every mismatch logged and resynced | OPEN — diffs apply cleanly and the book never crosses, but no venue reference is compared yet and the book is partial |
| Sequence gaps are handled | fewer than 1 unexplained gap per instrument day over the soak, every gap recorded and the window marked unreliable | PARTIAL — connection and update-ID gaps are recorded and marked, the rate over a soak is not measured |
| Processing latency | p50 and p99 socket read to book ready published per venue, target p99 under 5 ms on reference hardware | NOT MEASURED |
| Determinism | ten replays of one captured day produce byte identical normalised output and identical signals | OPEN |
| Cross machine reproducibility | the same experiment reproduces metrics exactly on a second machine | OPEN |

Scope: Binance and Bybit, BTC/USDT and ETH/USDT, spot and USDT perpetual.
Channels: book diff, book snapshot, trades, book ticker, funding, open interest,
liquidations.

Built so far: capture for Binance `book_diff`, chunked storage with a SHA-256
integrity index, reconnect with gap records, update-ID continuity checking, and
order-book reconstruction from captured frames.

## Gate 2 — does it work

Shadow mode compares predicted fills against what was actually tradable. Kill
if model error exceeds roughly 10 bps on liquid pairs.

## Gate 3 — does it produce useful research

One honest published research note. Kill if three months of real use does not
produce one substantive result.

## Gate 4 — do other people use it

Five or more external users running the recorder, at least two external
reproductions of a published result. Kill if a real launch sees no external use
within six months.

## Gate 5 — what do people struggle to do themselves

Fifteen or more structured interviews plus issue telemetry.

## Gate 6 — will they pay

Signed letters of intent or prepayments from three or more buyers before
anything commercial is built.

## Gate 7 — build the commercial product

Sell data first, then hosted sweeps, then calibrated execution models.
