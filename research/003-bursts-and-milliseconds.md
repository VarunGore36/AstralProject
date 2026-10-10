# Research note 003: the tape arrives in bursts, and milliseconds lie

Date: 2026-10-10 · Status: **observational, single 30-minute window.**
Companion to notes 001 (count vs volume) and 002 (dust vs whales). No
profitability claim; limits stated; method reproducible.

## Question

How is trade flow distributed in *time* — and what does the venue's own
timestamp resolution hide from anyone replaying it?

## Dataset

- 14,979 trade prints, Binance spot BTC/USDT, 30 minutes, one recorded
  reconnect gap mid-capture (`venue_close`, single attempt), audit verdict
  `healthy`, clean duration stop.
- Normalized to one Parquet file (14,979 exact-decimal rows); analysis over
  venue timestamps (`T`, millisecond resolution).

## Results

| Measure | Value |
|---|---|
| Span | 30.0 min |
| Prints | 14,979 |
| Inter-arrival, median | 0.0 ms |
| Inter-arrival, p90 / p99 | 399 ms / 1,736 ms |
| Max inter-trade gap | 4.4 s |
| Same-millisecond consecutive prints | 10,931 of 14,978 (73.0%) |
| Silent seconds (zero prints) | 320 of ~1,800 (17.8%) |
| Prints/sec, median / p99 / max | 2 / 189 / 529 |

## Reading

Three facts that jointly break naive time handling. First, flow is
violently bursty: the median second holds 2 prints while the busiest holds
529, and nearly a fifth of all seconds hold none. Any fixed-interval
representation (1-second bars, per-second features) averages a distribution
that never sits at its average — the bar is a fiction the tape never
traded. Second, 73% of consecutive prints share a millisecond, so ordering
*within* a millisecond is venue-opaque: the venue batches, and replay
order inside the batch is arrival order, not execution order. A fill model
that fills print-by-print inside a shared millisecond pretends to knowledge
it cannot have; conservative handling treats same-millisecond prints as
simultaneous. Third, the longest venue-quiet stretch is 4.4 seconds —
unremarkable, except beside the reconnect gap the capture layer recorded
separately: venue time cannot distinguish "nothing traded" from "we heard
nothing," which is exactly why gap records exist alongside timestamps.

What would strengthen or overturn this: burst statistics across sessions
(Asian/European/US hours separately); same-millisecond batch sizes by
venue (does Bybit batch the same way?); a fill model with explicit
same-millisecond simultaneity, backtested against the current
print-by-print default to measure how much phantom precision it removes.
