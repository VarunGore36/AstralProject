# Research note 014: memory strengthens with stress, sessions stay uniform

Date: 2026-10-16 · Status: **one extinction week, hourly buckets, stated
pairs.** Second crisis datapoint for note 013's open question: Luna/UST
death spiral (2022-05-08..14, BTC ~$35k→$26k), identical lag-1 method over
hourly taker-buy shares — overall, per session, and split by first-hour
magnitude.

## Question

Note 013 found r=+0.49 in the SVB week against +0.36 calm — suggestive,
not established. A second, harsher crisis decides whether the ordering
(stress strengthens memory) replicates or was noise.

## Dataset and method

- Seven daily spot aggTrades slices, 13,960,241 prints into 168 UTC
  hours, overall taker-buy 50.2% (balanced tape through a crash).
  Identical pairs to notes 011/013 (consecutive hours, same-session
  only, median magnitude splits). Pins are extracted-CSV SHA-256 in
  `research/scripts/stress_014.py` (stdlib only, one command).
- Daily flow ramps mid-week (3.0M/3.3M prints May 11–12 vs ~1M on the
  quiet days) — the stress is in the data, not assumed.

## Results

| Scope | Calm (011) | SVB stress (013) | Luna stress (014) |
|---|---|---|---|
| All consecutive hours | +0.36 ± 0.08 | +0.49 ± 0.08 | +0.62 ± 0.08 |
| Asia 00–08 | +0.34 ± 0.14 | +0.44 ± 0.14 | +0.67 ± 0.14 |
| EU 08–16 | +0.34 ± 0.14 | +0.43 ± 0.14 | +0.60 ± 0.14 |
| US 16–24 | +0.34 ± 0.14 | +0.60 ± 0.14 | +0.49 ± 0.14 |
| High-magnitude first | +0.32 ± 0.11 | +0.60 ± 0.11 | +0.75 ± 0.11 |
| Low-magnitude first | +0.03 ± 0.11 | +0.20 ± 0.11 | +0.29 ± 0.11 |

## Reading

Three facts, one upgrade and one refusal. First, the ordering replicates:
calm +0.36, SVB +0.49, Luna +0.62 — memory strengthens monotonically with
stress across three independent weeks, and the Luna-vs-calm gap (+0.26 at
~2.3 sigma) now clears the bar the SVB gap only approached. Second, the
magnitude mechanism replicates with it: +0.75/+0.29 high/low, the same
conviction-hours story at higher amplitude. Third, sessions refuse to rank:
Asia reads highest (+0.67) and US lowest (+0.49) — the reverse of the SVB
week — but no pairwise gap clears combined error bars, so the uniform
reading from note 011 stands extended, not overturned: memory is
session-flat within error in calm, SVB, and Luna weeks alike. The
epicenter moved (US hours for SVB, Asia hours for Terra) while the
uniformity did not — consistent with one tape-wide carryover process
whose level, not whose memory, follows events.

What would strengthen or overturn this: a third crisis (each additional
week is another independent draw — the honest unit is still small); the
perp cut in stress (does balanced flow remember any better when it
matters?); maker-side persistence; and the live-captured version, where
hours stop being archive buckets.

## Reproduce

```sh
python3 research/scripts/stress_014.py --data-dir /tmp/stress014
```

Seven CSV content hashes pinned in the script, refused on any mismatch —
verified by a from-scratch rerun with identical figures.
