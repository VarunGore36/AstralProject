# Research note 002: the tape is dust plus whales

Date: 2026-10-10 · Status: **observational, single 30-minute window.**
Companion to note 001 (count vs volume). Same honesty rules: no
profitability claim, limits stated, method reproducible.

## Question

What does the *size distribution* of taker prints imply for signal design?
Note 001 showed count and volume disagree; this note measures how the
volume itself is distributed.

## Dataset

- 14,979 trade prints, Binance spot BTC/USDT, 30 minutes.
- One connection gap mid-capture (`venue_close`, single attempt), recorded
  inline; audit verdict `healthy`, clean duration stop. The gap is part of
  the evidence: the pipeline met reality once and marked it.
- Normalized to one Parquet file (14,979 exact-decimal rows).

## Results

Overall: 7,911 buys (52.8%) vs 7,068 sells; 64.744 vs 62.826 BTC by volume
(50.8% buys). Per-minute buy shares swing 7.7%–94.5% by count — the
note-001 pattern persists over a 3x window.

Size distribution (BTC per print):

| Percentile | Size (BTC) | Approx. USD @ ~83k |
|---|---|---|
| median | 0.0001 | ~$8 |
| p90 | 0.0100 | ~$830 |
| p99 | 0.1840 | ~$15,300 |

The top 1% of prints by size carries **46.4% of total volume**.

## Reading

Half the prints are dust: the median print is ~$8, on which a 5 bps taker
fee is under half a cent — individually executable, informationally
worthless. Nearly half the volume arrives in 1% of prints. Any signal that
weights prints equally (counts, or unweighted averages) lets dust outvote
size by orders of magnitude; any signal that could matter must weight by
size or filter by it, and then faces the note-001 problem (size shows no
persistent side). Between dust that says nothing and size that says nothing
consistently, the per-print tape offers no tradeable signal in this
window — the third consecutive "no," each for a different measured reason.

What would strengthen or overturn this: size-conditioned side persistence
(do large prints lead short-horizon drift? — needs book context per print,
not available in trade-only captures); multi-venue size distributions;
fee-floor analysis per print-size bucket through the fill model.
