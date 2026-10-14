# Research note 008: a full year of carry, bear grind to bull ramp

Date: 2026-10-14 · Status: **twelve consecutive months, hourly bars,
stated assumptions.** The window-picking antidote to notes 004–007: all of
2023, January to December, unconditional hold plus the p90/p10 timed rule
from note 007 — same legs, fees, and fills throughout, so the year decides
between them instead of any quarter.

## Question

Notes 004–007 chose their windows (bull quarter, bear quarter, both
halves). Does the carry thesis survive a whole year it did not get to
choose — and does timing earn its turnover over twelve months?

## Dataset and method

- Binance public archive, 36 slices pinned by SHA-256 in
  `research/scripts/carry_008.py`: 1,095 funding payments plus 8,759
  paired hourly bars. Pairing is by timestamp, not position: March carries
  one perp-only bar (the 2023-03-24 spot halt), dropped and disclosed —
  the script asserts at most a handful of unpairable bars and reports the
  count (1).
- Two provenance upgrades over notes 004–007, both forced by findings:
  pins cover the extracted CSV bytes, not the zip container (the CDN
  re-packages identical CSVs into byte-different zips — observed on one
  file across three downloads with identical content), and every file
  asserts its calendar row count beside its hash (spot files carry no
  header while perp/funding do — found the hard way, documented in code).
- Unconditional hold over the full year ($20 in fees). Timed p90/p10 over
  the same bars with January doubling as the 720h warmup. Same $10k legs,
  5 bps taker, fills at closes, bronze labeling throughout.

## Results

| Measure | Unconditional hold | Timed p90/p10 |
|---|---|---|
| Round trips (fills) | 1 open + 1 close ($20) | ~12 (24 fills, $240) |
| Bars open | all 8,759 | 1,837 (23%) |
| Funding collected | $786.57 | $323.44 (41%) |
| Basis drift PnL | −$20.02 | +$158.04 |
| Net, 365 days on $20,000 | **+$746.55 (+3.73%)** | **+$241.48 (+1.21%)** |

Funding by month ($): Jan 80.00 · Feb 69.91 · Mar 52.44 · Apr 45.13 ·
May 51.29 · Jun 60.29 · Jul 56.28 · Aug 53.89 · Sep 9.74 · Oct 37.03 ·
Nov 101.84 · Dec 168.72. September — 33 of 90 payments negative — is the
year's dead month; November–December, zero negatives, is where the year
was made. Spot ran $16,529.67 → $42,283.58 across the window.

## Reading

Three facts that end the window debate. First, the unconditional hold
compounds across regimes: eleven of twelve months paid, September cost
little, and the year cleared 3.7% after all stated costs — the carry
thesis survives contact with a year nobody chose. Second, timing helped
drift again (+$158 vs −$20) and lost again (−$505 of gap): twelve careful
turns still cost more than the selectivity they bought, at $20 a turn.
Third, the fee mechanism is now visible across four studies: turnover is
the only variable that ever decides the sign. The trilogy's moral holds
at twelve months: timing is real edge on drift and negative edge after
turnover, and no band has yet been both selective and cheap.

What would strengthen or overturn this: maker fills where the book
supports them (the fee floor remains the entire game); sizing by
conviction; borrowing-inclusive inverse positions for negative-funding
regimes; and the live-captured version, where fills stop being assumed.

## Reproduce

```sh
python3 research/scripts/carry_008.py --data-dir /tmp/carry008
```

Thirty-six CSV content hashes plus calendar row counts pinned in the
script, refused on any mismatch — verified by a from-scratch rerun with
identical figures.
