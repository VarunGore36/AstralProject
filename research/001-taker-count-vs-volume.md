# Research note 001: taker count imbalance is not volume imbalance

Date: 2026-10-09 · Status: **observational, single 10-minute window.**
This note makes no profitability claim. It documents one measurement that
survived contact with the data and one naive signal that did not deserve to.

## Question

Do bursts of taker buy prints on Binance BTC/USDT spot constitute executable
buy pressure — or does the imbalance evaporate when weighted by size?

## Dataset

- 15,655 trade prints, Binance spot BTC/USDT, 10 minutes from 15:56:14 UTC.
- Capture `4327ed7c-da07-4018-9b77-19936411bba0`, 15,655/15,655 frames,
  zero gaps, `duration_elapsed`, verdict `healthy` on audit.
- Normalized to one Parquet file (15,655 rows, exact decimals); bucketed
  per minute by venue timestamp in the analysis step.
- Limitation stated up front: one venue, ten minutes, no out-of-sample
  window. The slice is not archived with this note — the method below plus
  any 10-minute slice reproduces the *procedure*, not these numbers.

## Method

`astra-record capture` (trade channel) → `check` (healthy) →
`astra-normalize` → per-minute buy share by print count and by volume.
No strategy, no fills, no fees in this note — it measures the signal's raw
material, which is where the naive version already fails.

## Results

| Minute | Prints | Buy share (count) | Buy share (volume) |
|---|---|---|---|
| 0 | 1,408 | 87.6% | 87.7% |
| 1 | 889 | 83.7% | 66.8% |
| 2 | 921 | 43.4% | 57.1% |
| 3 | 951 | 12.8% | 31.9% |
| 4 | 2,170 | 15.5% | 16.4% |
| 5 | 2,012 | 46.9% | 43.3% |
| 6 | 2,445 | 56.9% | 42.2% |
| 7 | 1,469 | 46.7% | 51.9% |
| 8 | 1,866 | 95.3% | 80.1% |
| 9 | 1,524 | 92.7% | 88.8% |
| **Total** | **15,655** | **57.8%** | **50.6%** |

46.931 BTC bought vs 45.776 BTC sold across the window.

## Reading

By count, the tape screams: minutes swing from 12.8% to 95.3% buys, and a
count-based "buy pressure" signal would flip sides repeatedly. By volume,
the same window sums to 50.6% buys — effectively flat. The imbalance lives
in small prints; size does not follow count.

The tradability consequence is immediate and needs no backtest: a signal
that flips this often pays taker fees on every flip. At 5 bps per side, a
strategy trading each minute's imbalance would need moves an order of
magnitude larger than anything visible here just to cover its own churn.
The signal dies at the cost model, before execution is even modeled. That
is a publishable "no" — exactly the kind this project exists to produce.

## What would strengthen or kill this

- A 24h+ window across venues (does the count/volume split persist?).
- Running the minute-flip rule through `exec-v1` with fee tiers to put a
  measured number on the churn death.
- Trade-size distribution per minute (are the small prints retail flow? —
  unanswerable from public prints alone; stated, not speculated).
