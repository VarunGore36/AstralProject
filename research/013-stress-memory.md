# Research note 013: memory survives the stress week, modestly stronger

Date: 2026-10-16 · Status: **one crisis week, hourly buckets, stated
pairs.** The stress test notes 011–012 left open: SVB collapse (Mar 10)
plus USDC-depeg weekend, ~54M prints at ~7× January's flow. Does
imbalance memory strengthen when it matters, and does the session split
survive contact with a crisis?

## Question

Calm-week memory (r=+0.36) could be a fair-weather artifact — microstructure
that evaporates when flow multiplies and regimes break. Same lag-1 method
over 2023-03-06..12, overall plus sessions plus magnitude splits.

## Dataset and method

- Seven daily spot aggTrades slices, 54,209,013 prints into 168 UTC
  hours, overall taker-buy 50.2%. Pins are extracted-CSV SHA-256 in
  `research/scripts/stress_013.py` (stdlib only, one command) — note the
  discipline explicitly, because this round caught a real slip: zip-level
  hashes were pasted where content hashes belong, and the script refused
  them before any math ran.
- Identical pairs to note 011 (consecutive hours, same-session only,
  median magnitude splits) so every number below compares directly.

## Results

| Scope | Calm week (011) | Stress week (013) |
|---|---|---|
| All consecutive hours | +0.36 ± 0.08 | +0.49 ± 0.08 |
| Asia 00–08 | +0.34 ± 0.14 | +0.44 ± 0.14 |
| EU 08–16 | +0.34 ± 0.14 | +0.43 ± 0.14 |
| US 16–24 | +0.34 ± 0.14 | +0.60 ± 0.14 |
| High-magnitude first | +0.32 ± 0.11 | +0.60 ± 0.11 |
| Low-magnitude first | +0.03 ± 0.11 | +0.20 ± 0.11 |

## Reading

Three facts with descending certainty. First, memory survives 7× flow:
+0.49 against 0.08 is unambiguously nonzero — the carryover process does
not evaporate under stress. Second, the magnitude gradient replicates and
steepens (+0.60/+0.20 vs +0.32/+0.03): conviction hours still carry
everything, and in a crisis week even quiet hours lean slightly. Third —
stated as unresolved, not as a finding — the session split does not
replicate cleanly: the US reads +0.60 against +0.44/+0.43, but the gaps
do not clear combined error bars, and neither does the +0.13 overall
strengthening (+0.49 vs +0.36 at ~1.1 sigma). "Stronger under stress" is
suggestive, not established; what is established is that memory persists
at crisis volume with the same shape (magnitude-graded, session-flat
within error).

What would strengthen or overturn this: more stress weeks (one crisis is
an anecdote with error bars — the honest unit here is still n=167 hours);
the perp cut in stress (does balanced flow remember any better when it
matters?); maker-side persistence; and the live-captured version, where
hours stop being archive buckets.

## Reproduce

```sh
python3 research/scripts/stress_013.py --data-dir /tmp/stress013
```

Seven CSV content hashes pinned in the script, refused on any mismatch —
verified by a from-scratch rerun with identical figures.
