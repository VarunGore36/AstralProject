# Research note 005: the same carry, in a hostile regime, earns nothing

Date: 2026-10-13 · Status: **single 90-day window, hourly bars, stated
assumptions.** Mirror of note 004 with identical method, opposite weather:
October–December 2022, when funding averaged near zero and went negative
in November. A negative result with leashed claims.

## Question

Note 004 found long-spot/short-perp carry clearing its costs 28-to-1 in
Q1 2024. Was that the strategy or the weather? This note reruns the exact
unconditional hold — $10k legs, first/last-close fills, 5 bps taker, no
borrow model — over the three months containing the FTX collapse.

## Dataset

- Binance public archive (`data.binance.vision`), three slices pinned by
  SHA-256 in `research/scripts/carry_005.py` (independent script, same
  math; note 004's script untouched): 276 funding payments plus 2,208
  hourly bars each for spot and perp, hour-aligned with 0 mismatches
  checked in analysis.
- Same bronze labeling as note 004: archive data, no capture provenance.

## Assumptions

Identical to note 004 in every cell ($20k unlevered capital, 4 fills at
5 bps = $20.00, zero beyond-fee slippage assumed, fills at closes). The
comparison is clean by construction: only the window changed.

## Results

| Measure | Value |
|---|---|
| Window | 2022-10-01 → 2022-12-31, 276 funding payments |
| Mean funding rate | 0.28 bps per 8h payment (vs 2.04 in Q1 2024) |
| Funding received, by month | Oct +$53.08 · Nov −$17.37 · Dec +$42.36 |
| Negative payments | 8/93 · 32/90 · 11/93 |
| Funding received, total | $78.08 |
| Entry / exit fees | $20.00 (26% of funding, vs 3.6% before) |
| Basis (perp − spot), entry → exit | −$9.78 → −$4.80 (discount narrowed) |
| Basis drift PnL | −$2.57 |
| Net, 90 days on $20,000 | **+$55.51 (+0.28%)** |
| Daily Sharpe (analytic floats) | 4.35 — descriptive only, see limits |

## Reading

Two facts, both against the strategy. First, the fee ratio tells the
whole story before anything else: at 26% of funding versus 3.6%, the same
$20 of costs that was rounding error in Q1 2024 eats a quarter of the
carry here. Carry does not have a return; it has weather, and fees are
constant. Second, the net — fifty-five dollars on twenty thousand over
ninety days — sits inside every unmodeled cost at once: one basis point
of slippage per fill, one hour of mistimed entry, one revised fee tier,
and the sign flips. This is not "carry earns 0.28%"; it is "carry earns
nothing here, and the nothing is measured."

Together the notes frame the question honestly: the strategy survives its
costs only when funding runs hot, and funding runs hot only sometimes.
What would strengthen or overturn this: the basis-percentile entry timing
both notes defer (does selectivity buy back the dead quarters?); a full
bear-to-bull year rather than two chosen windows; and the live-captured
version the 06b protocol specifies, where fills stop being assumed.

## Reproduce

```sh
python3 research/scripts/carry_005.py --data-dir /tmp/carry005
```

Nine archive SHA-256 hashes pinned in the script, refused on any other
bytes — same provenance discipline as note 004.
