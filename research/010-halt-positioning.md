# Research note 010: positioning saw the flush, then flipped while spot was dark

Date: 2026-10-15 · Status: **seven days, 5-minute metrics, stated ratios.**
The positioning study the strategy flagged on day one and never ran: open
interest plus account long/short ratios around the 2023-03-24 spot halt
(matching-engine bug, ~11:00–13:00 UTC; perps never stopped). Same
provenance discipline as the carry series, smaller data.

## Question

Did positioning anticipate the halt-week stress, and what did it do while
spot was dark — orderly hedging, or a flip?

## Dataset and method

- Seven daily metrics slices (2023-03-20..26), 288 five-minute rows each:
  `sum_open_interest` (BTC), `count_long_short_ratio` (accounts, equal-
  weighted — the note-002 dust caveat applies: one dust account counts
  like one whale), top-trader ratios for the smart-money check, plus
  March hourly closes for price context. Nine archive SHAs pinned in
  `research/scripts/positioning_010.py` (stdlib only, one command); the
  two klines pins match note 008's verified pins for the same files.
- Ratios are long÷short account counts: below 1.0 means short-heavy books.

## Results

| Day | OI open → close (change) | L/S open → close, min/max |
|---|---|---|
| Mar 20 | 111,990 → 111,212 (−779) | 0.77 → 0.77, 0.69/0.88 |
| Mar 21 | 111,130 → 110,509 (−621) | 0.77 → 0.73, 0.67/0.83 |
| Mar 22 | 110,480 → 102,303 (−8,177) | 0.73 → 0.85, 0.69/0.86 |
| Mar 23 | 102,326 → 103,900 (+1,574) | 0.85 → 0.70, 0.61/0.88 |
| Mar 24 (halt) | 104,107 → 98,060 (−6,048) | 0.70 → 0.94, 0.70/0.94 |
| Mar 25 | 98,122 → 97,209 (−912) | 0.94 → 0.94, 0.89/0.97 |
| Mar 26 | 97,234 → 102,629 (+5,396) | 0.94 → 0.83, 0.83/0.94 |

Midday closes: Mar 22 $28,220 → Mar 23 $27,470 → Mar 24 $28,080 (spot).
On the halt day itself, top-trader ratios tracked the crowd ratio tick
for tick (0.68 → 0.96) — no smart-money divergence anywhere in the week.

## Reading

Three facts, one non-finding. First, the flush preceded the halt: −8,177
BTC of open interest (−7.4%) left on Mar 22 during the selloff, the
largest daily move of the week — positioning de-risked a full day before
the matching engine broke. Whether it saw anything coming or simply
reacted to price is indistinguishable here, and the note does not pretend
otherwise. Second, the flip happened while spot was dark: the account
ratio climbed 0.70 → 0.94 through Mar 24, including an intraday OI dip to
103,192 around the halt window itself — shorts covered (or longs added)
into a tape that was not printing. Venue-halting while the parallel market
stays open does not freeze positioning; it relocates it. Third, the null:
top traders moved with the crowd all week, which bounds what the ratio
series can claim — it measures the crowd, including its dust, and the
crowd is all it measures.

What would strengthen or overturn this: notional-weighted positioning
(the dust caveat, answered with size data); liquidation prints around the
flush (no sanctioned archive carries them — checked, both monthly and
daily paths 404); the same week on a second venue; and funding alongside
(negative prints during the flush would complete the squeeze picture).

## Reproduce

```sh
python3 research/scripts/positioning_010.py --data-dir /tmp/pos010
```

Nine archive SHA-256 hashes pinned in the script, refused on any mismatch
— verified by a from-scratch rerun with identical figures.
