# Research note 006: timing the carry by premium percentile improves drift and dies on fees

Date: 2026-10-13 · Status: **two pinned windows, hourly bars, stated
assumptions.** Answers the timing question both carry notes deferred: enter
only when the perp premium sits at/above its trailing 75th percentile,
exit below the trailing median — trailing-only percentiles (720h minimum
warmup, no look-ahead), same legs, fees, and fills as notes 004/005 so
only the entry rule changed.

## Question

Notes 004/005 found carry surviving costs only when funding runs hot. Can
selectivity buy back the dead quarters — harvest the rich-premium hours,
sit out the rest?

## Dataset and method

- The pinned slices behind notes 004 (Q1 2024) and 005 (Q4 2022): 549
  funding payments, 4,392 hourly bars per leg, 0 misaligned hours.
- The nine-month hole between the quarters (2023-01..09, no pinned data)
  splits the study into two contiguous blocks; no state crosses it —
  percentiles, warmup, and positions all reset per block. A rule that
  traded across unobserved months would be measuring its own absence.
- Baseline rerun per block: unconditional hold over the same post-warmup
  windows (open + close per block = $40 in fees, not $20 — two blocks).
- Timed rule: hysteresis band (enter ≥ trailing p75, exit < trailing p50),
  funding credited when open at the payment timestamp, fills at closes.

## Results

| Measure | Unconditional baseline | Timed (p75/p50) |
|---|---|---|
| Round trips (fills) | 2 opens + 2 closes ($40) | ~163 (326 fills, $3,260) |
| Bars open | all 2,952 | 1,841 (62%) |
| Funding collected | $472.52 | $454.89 (96% of baseline) |
| Basis drift PnL | −$52.59 | +$255.45 |
| Net | **+$379.93** | **−$2,549.66** |

## Reading

Two facts pulling opposite ways, and the fees win by an order of
magnitude. First, selectivity does what it promises on market terms: it
collects 96% of the funding in 62% of the time and turns basis drift from
−$53 into +$255 — rich-premium hours really are the hours worth holding.
Second, it pays 163 round trips for the privilege: $3,260 in fees against
$455 of harvested funding, a round trip every ~18 hours at $20 a turn.
The unconditional hold wins not by being smarter but by barely trading.
The honest version of "timing works": timing the premium is real edge on
drift and negative edge after turnover, at $10k size and 5 bps a fill.

What would strengthen or overturn this: wider hysteresis (fewer turns —
done as note 007, same script, two flags); maker instead of taker
fills where the book supports it; a full consecutive year so no block
splitting is needed; and the live-captured version, where fills stop being
assumed and start being simulated.

## Reproduce

```sh
python3 research/scripts/carry_006.py --data-dir /tmp/carry006
```

Eighteen archive SHA-256 hashes pinned in the script (the nine behind
each of notes 004/005, re-enforced on every download), refused on any
other bytes.
