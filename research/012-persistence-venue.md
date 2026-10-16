# Research note 012: memory lives in the imbalance, and spot remembers best

Date: 2026-10-16 · Status: **one week per venue, hourly buckets, stated
pairs.** Closes both halves note 011 deferred: the same lag-1 cut on perp,
and magnitude-conditioned splits on each tape (pairs starting from
above/below-median |share − 0.5| hours).

## Question

Note 011 found spot imbalance memory identical across sessions. Two
refinements compete for the mechanism: balanced perp flow may forget
faster, and persistence may concentrate where imbalance is strongest. Both
are tested on the note-009 weeks (spot and perp aggTrades, 2024-01-15..21).

## Dataset and method

- Fourteen daily slices pinned by extracted-CSV SHA-256 in
  `research/scripts/persist_012.py` — the exact pins behind note 009 for
  the same files, re-verified on download here. Stdlib only, one command.
- Lag-1 Pearson on hourly taker-buy shares overall, plus first-hour
  magnitude splits (median cut, ~84/83 pairs each). Standard errors beside
  every r; the low-n splits are read with their error bars, not past them.

## Results

| Scope | Pairs | r | ±1 SE |
|---|---|---|---|
| Spot, all hours | 167 | +0.36 | 0.08 |
| Spot, high-magnitude first | 84 | +0.32 | 0.11 |
| Spot, low-magnitude first | 83 | +0.03 | 0.11 |
| Perp, all hours | 167 | +0.12 | 0.08 |
| Perp, high-magnitude first | 84 | +0.17 | 0.11 |
| Perp, low-magnitude first | 83 | −0.05 | 0.11 |

## Reading

Two facts that locate the memory. First, persistence concentrates
entirely in imbalance: high-magnitude hours carry the full +0.32 while
quiet hours read +0.03 — white noise with a standard error attached.
Whatever carries over is carried by conviction hours, not by the tape in
general. Second, balanced flow nearly forgets: perp manages +0.12 overall
against a 0.08 error bar, a third of spot's memory with the same
magnitude gradient (+0.17 vs −0.05). Combined with note 009's flat 50%
perp sessions, the venue picture is now complete — perp flow is bigger,
smoother, and more forgetful; spot flow is noisier, directional, and
remembers where it was loud. A signal built on persistence belongs on
spot, sized by conviction, and nowhere else in this dataset.

What would strengthen or overturn this: more weeks (84-pair splits are
thin — the high/low gap on spot clears its bars, on perp it does not);
the same cut in a stress week (does memory spike when it matters?);
maker-side persistence (resting-order imbalance rather than taker prints);
and the live-captured version, where hours stop being archive buckets.

## Reproduce

```sh
python3 research/scripts/persist_012.py --data-dir /tmp/persist012
```

Fourteen CSV content hashes pinned in the script (the note-009 pins),
refused on any mismatch — verified by a from-scratch rerun with
identical figures.
