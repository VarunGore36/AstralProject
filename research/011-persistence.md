# Research note 011: imbalance persists, equally, in every session

Date: 2026-10-16 · Status: **one week, hourly buckets, stated pairs.**
Answers the persistence half of note 009's deferred questions: spot
taker-buy share varies by session — but does an imbalanced hour predict
the next one, and does that memory itself vary by session?

## Question

Note 009 found spot direction varying by session (43.9% → 37.6%
taker-buy, Asia to US) while perp holds still. Two readings compete: each
session has its own flow dynamics, or one tape-wide memory wearing three
different levels. Lag-1 autocorrelation of hourly taker-buy share,
overall and per session, separates them.

## Dataset and method

- The same seven spot aggTrades slices behind note 009 (2024-01-15..21),
  repinned here (identical pins, re-verified on download): 7,553,902
  prints bucketed into 168 UTC hours, overall taker-buy 40.3%.
- Pairs are consecutive hours with both hours inside one session
  (49 pairs each; 167 overall including boundaries). No cross-boundary
  pairs — midnight effects are not smuggled into any session.
- Pearson r on shares plus r on imbalance magnitude (|share − 0.5|),
  each with 1/√n beside it. Correlation is descriptive here, not a model.

## Results

| Scope | Pairs | r (share) | r (magnitude) | ±1 SE |
|---|---|---|---|---|
| All consecutive hours | 167 | +0.36 | +0.32 | 0.08 |
| Asia 00–08 | 49 | +0.34 | +0.26 | 0.14 |
| EU 08–16 | 49 | +0.34 | +0.31 | 0.14 |
| US 16–24 | 49 | +0.34 | +0.27 | 0.14 |

## Reading

Two facts that jointly retire the question. First, memory is real and
modest: +0.36 overall against a 0.08 standard error — an imbalanced hour
leans the same way about a third of the time beyond chance, on this week,
at this scale. Second, memory does not vary by session within error: the
three sessions read identically while their levels differ by six points.
Direction varies by session (note 009 stands); memory does not. The tape
has one persistence mechanism wearing three different levels — the
simplest consistent story is a single intraday carryover process (open
positions, working orders, Asia-to-US handoff) modulated by who is awake,
not three different microstructures.

What would strengthen or overturn this: more weeks (49 pairs per session
is thin — the error bars admit anything from +0.2 to +0.5); the same cut
on perp (balanced flow may forget faster); magnitude-conditioned splits
(does persistence concentrate in strongly imbalanced hours?); and the
live-captured version, where hours stop being archive buckets.

## Reproduce

```sh
python3 research/scripts/persist_011.py --data-dir /tmp/persist011
```

Seven CSV content hashes pinned in the script (the spot pins behind note
009), refused on any mismatch — verified by a from-scratch rerun with
identical figures.
