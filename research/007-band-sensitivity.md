# Research note 007: wider bands bleed slower, and still lose to doing nothing

Date: 2026-10-13 · Status: **same two windows, five hysteresis bands,
hourly bars, stated assumptions.** The follow-up note 006 promised ("wider
hysteresis — the obvious next run, same script, two constants"): enter at
p75/p90, exit at p50/p25/p10, everything else identical — same legs, fees,
fills, trailing-only percentiles, per-block discipline.

## Question

Note 006 found selectivity improving drift and dying on turnover. Is
turnover a tunable cost — do wider bands rescue the timed rule?

## Method

`research/scripts/carry_006.py` grew `--enter-pct`/`--exit-pct` flags
(defaults reproduce note 006 exactly: net −$2,549.66, verified). Five
bands run against the same 549 payments and 4,392 hourly bars. All five
results are reported below — selecting only the best would be the very
p-hacking this series exists to refuse.

## Results

| Band (enter/exit) | Round trips | Bars open | Funding | Drift | Fees | Net |
|---|---|---|---|---|---|---|
| unconditional hold | 2 | 2,952 | $472.52 | −$52.59 | $40.00 | **+$379.93** |
| p75 / p50 | ~163 | 1,841 | $454.89 | +$255.45 | $3,260 | −$2,549.66 |
| p90 / p50 | ~80 | 1,578 | $442.69 | +$151.90 | $1,600 | −$1,005.42 |
| p75 / p25 | ~69 | 2,175 | $468.30 | +$121.49 | $1,380 | −$790.22 |
| p90 / p25 | ~40 | 1,932 | $458.90 | +$82.62 | $800 | −$258.48 |
| p90 / p10 | ~21 | 2,189 | $465.08 | +$35.66 | $420 | **+$80.75** |

## Reading

The gradient is the finding, not any single row. Every column moves
monotonically with band width: fewer turns, less fee bleed, smaller drift
edge, funding share steady near 100%. Turnover is the dominant variable —
at $20 a round trip, even 21 turns cost more than the entire drift edge
they buy. The widest band scrapes positive (+$80.75) while still trailing
the unconditional hold by $300, and that $80 sits inside the same noise as
note 005's +$55: one basis point of slippage per fill erases it four times
over. Selectivity is confirmed real (drift improves in every timed row)
and confirmed insufficient (fees dominate in every timed row). There is no
band width where both statements stop being true at once — the fee per
turn is a floor, and the premium does not pay it often enough.

What would strengthen or overturn this: maker fills where the book
supports them (the fee floor is the whole game — halve it and rerun the
table); sizing turns by conviction instead of fixed notional; and the
live-captured version, where the turns stop being assumed.

## Reproduce

```sh
python3 research/scripts/carry_006.py --data-dir /tmp/carry006 --enter-pct 90 --exit-pct 10
```

Defaults reproduce note 006; flags walk the table. Eighteen archive
SHA-256 hashes pinned, refused on any other bytes.
