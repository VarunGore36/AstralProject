# Research note 004: BTC funding carry survived its costs — once, on one window

Date: 2026-10-12 · Status: **single 90-day window, hourly bars, stated
assumptions.** Companion to notes 001–003. A positive result with leashed
claims: what follows is what one window says, not what a strategy earns.

## Question

Can a simple delta-neutral BTC funding trade — long spot, short perp,
collecting funding — survive realistic fees and basis drift? The strategy
documents nominate this as research target #1 (data-cheap, maker-heavy,
honest at small size). This note runs the smallest honest version: buy and
hold both legs for one quarter, no timing, no leverage.

## Dataset

- Binance public archive (`data.binance.vision`), three slices pinned by
  SHA-256 in `research/scripts/carry_004.py`, which downloads, verifies,
  and analyzes in one command (stdlib only):
  - `futures/um/monthly/fundingRate/BTCUSDT` 2024-01..03 (273 payments)
  - `spot/monthly/klines/BTCUSDT/1h` 2024-01..03 (2,184 hourly bars)
  - `futures/um/monthly/klines/BTCUSDT/1h` 2024-01..03 (2,184 hourly bars,
    hour-aligned with spot: 0 mismatched hours checked in analysis)
- Not Astra-captured data: this predates the recorder and arrives without
  capture provenance, so it is analyzed as-is and labeled bronze. Nothing
  here touches the venue WebSocket path.

## Assumptions (all of them, up front)

| Assumption | Value |
|---|---|
| Legs | long $10,000 BTC spot + short $10,000 BTC perp notional |
| Capital | $20,000 unlevered (both legs fully backed) |
| Entry / exit | first / last hourly close of the window, filled in full |
| Fees | 5 bps taker per fill × 4 fills = $20.00 total |
| Borrow | none needed (long spot, short perp; no margin position) |
| Funding credit | each 8h payment × $10k notional, sign-correct for the short |
| Slippage beyond fees | 0 (hourly closes, $10k size — a stated simplification) |
| Liquidation | impossible unlevered; stated, not modeled |

## Results

| Measure | Value |
|---|---|
| Window | 2024-01-01 → 2024-03-31, 273 funding payments |
| Mean funding rate | 2.04 bps per 8h payment |
| Funding received, by month | Jan $111.92 · Feb $130.80 · Mar $314.02 |
| Funding received, total | $556.74 |
| Entry / exit fees | $20.00 (3.6% of funding) |
| Basis (perp − spot), entry → exit | $28.27 → $82.99 (premium widened) |
| Basis drift PnL | −$12.88 (widening hurts long-spot/short-perp) |
| Net, 90 days on $20,000 | **+$523.85 (+2.62%)** |
| Daily Sharpe (analytic floats) | 20.1 — descriptive only, see limits |

## Reading

Three facts, none of them a strategy. First, funding did the work and
fees barely mattered at this size: $20 of costs against $557 of carry is
a 28-to-1 ratio, the opposite of the short-horizon fee death in notes
001–003. Carry lives where turnover dies. Second, the basis worked against
the position the whole way — the premium nearly tripled — and the trade
still cleared by an order of magnitude, because funding dwarfed drift in
this window. A window where the premium compresses instead would read very
differently, and the analysis prices exactly that: every dollar of basis
widening is −$0.24 at this size. Third, the Sharpe is real arithmetic and
almost meaningless as a statistic: 90 days of smooth funding in a bull
quarter produce a smooth equity curve by construction. It describes this
window; it predicts nothing.

What would strengthen or overturn this: the same computation on a
negative-funding regime (a quarter where shorts pay, not collect); entry
timing by basis percentile instead of calendar; borrow-inclusive variants
for the inverse position; execution at top-of-book rather than closes;
and — the honest version of this question — the full loop: record spot
and perp live, normalize, replay the fills, and compare predicted carry
against collected carry.

## Reproduce

```sh
python3 research/scripts/carry_004.py --data-dir /tmp/carry004
```

The script pins nine archive SHA-256 hashes and refuses to run on any
other bytes — including the one transcription slip caught during writing,
which is exactly the failure mode the pinning exists for.
