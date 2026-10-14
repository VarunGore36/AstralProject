# Research note 009: the tape keeps venue hours, and batches on both venues

Date: 2026-10-14 · Status: **one full week, two tapes, stated sessions.**
Answers the two follow-ups notes 001–003 deferred that archive data can
answer: burst statistics split by session, and same-millisecond batching
by venue. The third (simultaneity in the fill model) stays open — it needs
the engine, not a week of bars.

## Question

When does the market actually trade — and does the millisecond-batching
note 003 found on spot also happen on perp? One week of Binance BTCUSDT
aggTrades (2024-01-15..21, Monday to Sunday), spot and USDT-perp side by
side: ~15.1M prints, streamed and never held. Sessions are UTC thirds
(Asia 00–08, EU 08–16, US 16–24), stated so the buckets can be argued with.

## Dataset and method

- Spot: 7,553,902 prints, $10.06B notional. Perp: 6,524,257 prints,
  $66.57B notional. Fourteen daily slices pinned by extracted-CSV SHA-256
  in `research/scripts/sessions_009.py` (stdlib only, one command).
- Timestamps asserted non-decreasing per file; pairing is per-print event
  time, never wall clock. Spot files carry no header, perp files do;
  spot booleans read `True`, perp `true` — the script normalizes both,
  and a 100%-taker-buy reading during writing caught the case difference
  before it reached this note.

## Results

| Measure | Spot week | Perp week |
|---|---|---|
| Prints | 7,553,902 | 6,524,257 |
| Taker-buy share | 40.3% | 50.1% |
| Same-millisecond prints | 45.5% (889,556 runs) | 49.6% (614,429 runs) |
| Prints/sec, median / max | 4 / 3,091 | 5 / 2,256 |
| Weekend share of prints | 15.7% (of 28.6% of time) | 11.3% |

Prints and volume by session (share of venue week):

| Session (UTC) | Spot prints | Spot volume | Spot taker-buy | Perp prints | Perp volume | Perp taker-buy |
|---|---|---|---|---|---|---|
| Asia 00–08 | 21.8% | 21.1% | 43.9% | 20.3% | 18.9% | 50.2% |
| EU 08–16 | 36.3% | 35.2% | 41.3% | 35.6% | 35.9% | 50.2% |
| US 16–24 | 41.9% | 43.7% | 37.6% | 44.2% | 45.2% | 49.9% |

## Reading

Three facts, each with a consequence. First, the week has a timezone: the
US third carries 42–44% of prints on both tapes while Asia carries ~20%.
Any feature averaged over "the day" mixes two different markets; session
tags belong on every print-level feature the way venue tags already do.
Second, spot shows session directionality and perp does not: spot
taker-buy share slides 43.9% → 37.6% across the day while perp sits at
50% in all three sessions. Direction lives in spot flow; perp flow is
balanced size passing through — consistent with hedging and basis traffic
rather than directional betting, and with note 002's dust (equal-weighted
signals overweight the noisiest, least directional flow). Third, batching
is a venue property, not a spot property: ~half of all prints share their
millisecond on both tapes, so replay order inside the millisecond is
arrival order on perp exactly as on spot. The simultaneity question for
the fill model just got twice as urgent — and the remaining venue in it
is Bybit, which publishes no archive to check against.

What would strengthen or overturn this: the same week on a volatility
breakout (does the US share stretch further?); Bybit batch rates from a
live capture, since no archive exists; taker-buy persistence (does an
imbalanced hour predict the next one, per session?); and the fill model
with explicit same-millisecond simultaneity, backtested against the
print-by-print default.

## Reproduce

```sh
python3 research/scripts/sessions_009.py --data-dir /tmp/sess009
```

Fourteen CSV content hashes pinned in the script, refused on any mismatch
— verified by a from-scratch rerun with identical figures.
