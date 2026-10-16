# Research note 016: the inverse fails in its best month, before borrow

Date: 2026-10-16 · Status: **one month, hourly bars, stated borrow range.**
Fills the empty cell in the carry matrix: every carry note so far ran
long-spot/short-perp. Short-spot/long-perp collects funding when rates go
negative — November 2022, 32 of 90 payments negative, is the kindest
possible window. Same legs, fees, and fills; borrow added as an explicit
sensitivity (0/2/5/10% APR over the 30-day hold) because short spot pays
borrow and the rate is an assumption, not data.

## Question

Does the inverse survive where the standard direction cannot — a
negative-funding month — once borrow is priced?

## Dataset and method

- November 2022 funding (90 payments) plus hourly closes, spot
  $20,456.57 → $17,163.64 through the FTX collapse aftermath. Three
  slices pinned by extracted-CSV SHA-256 in
  `research/scripts/inverse_016.py` (stdlib only, one command).
- Fees: 4 fills at 5 bps taker = $20.00. Basis drift signed for the
  inverse direction (gains when the discount narrows). Borrow pro-rated
  over exact days held at each stated APR.

## Results

| Measure | Value |
|---|---|
| Funding received (inverse) | +$17.37 |
| Fees | $20.00 |
| Basis drift PnL | +$0.41 (discount −$11.17 → −$10.34) |
| Net at 0% borrow | **−$2.23** |
| Net at 2% / 5% / 10% borrow | −$18.67 / −$43.32 / −$84.42 |

## Reading

Two facts, and the second is redundant by design. First, the inverse
fails before borrow exists: $17.37 of the kindest funding on record
against $20 of unavoidable fees nets −$2.23 with borrow at zero. No rate
assumption is load-bearing because the position is already underwater
without one — the sensitivity table exists to show that borrow only
deepens a hole fees already dug. Second, the matrix is now complete and
asymmetric: long/short survives hot funding at 28-to-1, the inverse dies
in cold funding at any borrow rate including zero. Carry is a one-
directional trade at retail size, and the direction is whichever side
funding pays.

What would strengthen or overturn this: a deeper negative-funding regime
(the sample is one month — though it is the most negative month in the
two years surveyed); maker fills halving the $20 floor (the fee boundary
again, as everywhere in this series); and the live-captured version,
where fills stop being assumed.

## Reproduce

```sh
python3 research/scripts/inverse_016.py --data-dir /tmp/inv016
```

Three CSV content hashes pinned in the script, refused on any mismatch —
verified by a from-scratch rerun with identical figures.
