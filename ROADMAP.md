# Roadmap

## Gate 1 — capture and reconstruct

Build a small system that can record live market data losslessly and prove its
own book reconstruction is correct.

| Claim | Measurable done | State |
|---|---|---|
| Capture is lossless | zero dropped raw frames over a 72 hour soak across all instruments, verified by websocket sequence continuity and a file integrity manifest | OPEN — short runs clean on Binance spot channels and Bybit spot+perp, the `check` audit tooling is built and proven on them, no soak yet |
| Book reconstruction is correct | local book proven against independent venue references (top-of-book and full-depth), every mismatch investigated and logged | PARTIAL — Binance diffs apply cleanly with file snapshots, Bybit reconstructs from in-band snapshots (1 bootstrap + 423 diffs, 0 gaps, live-verified), top-of-book matches the venue on levels 1–2 across 300/300 checks. Full-depth exactness proven with complete REST bootstrap on one connection (149/150 at depth 10, one dust transient); partial bootstraps diverge by rank drift, two connections by server disagreement |
| Sequence gaps are handled | fewer than 1 unexplained gap per instrument day over the soak, every gap recorded and the window marked unreliable | PARTIAL — Binance span gaps and Bybit version gaps are recorded and marked on both venues, the rate over a soak is not measured |
| Processing latency | p50 and p99 socket read to book ready published per venue, target p99 under 5 ms on reference hardware | PARTIAL — store half (p99 ~1ms) and book half (Binance p99 68µs, Bybit p99 32µs) measured live on two venues, both far under target. Not yet a published reference benchmark on fixed hardware |
| Determinism | ten replays of one captured day produce byte identical normalised output and identical signals | PARTIAL — the normalizer is proven byte-deterministic (same capture twice, identical SHA-256, CI-enforced); replay engine built through CLI with seed-independent live hashes; ten-replay ritual performed on a live 151-frame capture with ten identical hashes; cross-machine proof still open |
| Cross machine reproducibility | the same experiment reproduces metrics exactly on a second machine | OPEN — procedure defined in `docs/cross-machine-repro.md` (healthy verdicts + Parquet tree hash + signal_hash must all match); no second-machine run yet |

Scope: Binance, Bybit, and Coinbase spot, BTC/USDT, ETH/USDT, and BTC/USD, spot and USDT perpetual.
Channels: book diff, book snapshot, trades, book ticker, funding, open interest,
liquidations.

Built so far: capture for six mapped Binance channels plus Bybit spot and perp
`book_diff`, chunked storage with a SHA-256 integrity index, reconnect with gap
records, update-ID continuity checking, order-book reconstruction with snapshot
bootstrap, a reference comparison harness that reports matches and mismatches
per snapshot, an offline capture audit (`check`) that verifies hashes,
sequence and update-ID continuity without loading the whole capture into
memory, adversarial parser tests against hostile payloads, and real captured
venue frames committed as fixtures so CI replays reality instead of inventions.
Coinbase probed: `level2` needs authentication (no book without API keys),
`ticker` + `matches` are public but carry no checkable sequence, and both are
now captured live. Normalization implemented for `book_diff`, `trade`, and `top_of_book`
(exact decimals, whole-batch validation, Hive-partitioned output, bundle
expansion with print_index) and proven byte-deterministic
in CI and live. Replay engine built through a CLI with `book-top` and
`trade-tally` strategies (book diffs, trade prints with bundle expansion,
top-of-book, gap passthrough with book-breaking semantics, per-type
counters); live hashes seed-independent across ten
repetitions. The ten-replay
ritual stands performed; cross-machine proof awaits a second machine.
Conservative execution (`exec-v1`: limit-maker fills on trade-print-through,
required fee tiers, gaps void, plus a probe CLI) and a benchmark harness
(one replay pass plus probe set into a canonical hashed report, with a local
registry whose `verify` reproduces runs exactly) are built and tested; a
Python bindings foothold (`astra` module: exact decimals, clock) is
embedded-tested with packaging open. A full-pipeline audit with ranked
findings (mostly closed since) and one published research note (taker count
vs volume, negative result) round out the loop so far.

Order-book semantics match the venue's documented procedure exactly: events with
`u <= lastUpdateId` are discarded, and `U > lastUpdateId + 1` means events were
missed. Channels without a native stream are refused rather than guessed at.

## Gate 2 — does it work

Shadow mode compares predicted fills against what was actually tradable. Kill
if model error exceeds roughly 10 bps on liquid pairs.

## Gate 3 — does it produce useful research

One honest published research note. Kill if three months of real use does not
produce one substantive result.

Early, honestly scoped contributions so far (single windows each, not yet
substantive by the gate's own bar): notes 001–003 (count vs volume, dust
vs whales, bursts and milliseconds) — a series of negative results with
stated limits. The gate stays open until months of real use produce
something substantive.

## Gate 4 — do other people use it

Five or more external users running the recorder, at least two external
reproductions of a published result. Kill if a real launch sees no external use
within six months.

## Beyond Gate 4

Further stages are defined from what Gate 4 teaches, not before. The public
roadmap ends where the evidence ends.
