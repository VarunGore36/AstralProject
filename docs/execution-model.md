# Execution model v1 (conservative fills, limit-maker only)

Status: **specification only, not implemented.** It exists so the first
implementation judges fills by written rules instead of improvising them.
Replay stays a re-emitter (`docs/replay-design.md`); this model consumes
replay output and answers: "would this resting order have filled, and at
what cost?" Nothing here executes anything — there is no order submission
in this project.

## Scope

- Limit orders only: side, limit price, quantity (all fixed-point, matching
  `Fixed`). Market orders, stop orders, and cancels are explicitly out of
  scope for v1.
- One venue, one instrument, one replay stream at a time. Cross-exchange
  settlement (transfer delays, inventory) is a research question, not a fill
  rule — see the carry/arbitrage notes.

## The fill rule (Tier B, the default)

A resting buy limit at price `P` fills **if and only if** a later *trade
print* trades at or through `P` (print price `<= P`). A resting sell fills
on a later print at `>= P`. Quote-only movement never fills anything — the
"never assume fills" rule made mechanical.

- v1 fills the whole order at the limit price on the **first** through-print.
  Partial fills across prints are out of scope; an order that never sees a
  through-print expires unfilled at stream end.
- A through-print implies the market traded into the resting order, so v1
  fills are **maker** fills. The fee tier is a required input (basis points,
  per venue), defaulting to taker-with-no-rebate whenever the caller does not
  state otherwise — optimism must be explicit.
- Any gap marker (`synthetic = true`) overlapping the order's live window
  voids it: no fill is claimed across a hole. A replay that papers over gaps
  measures a market that never existed.

## Costs applied per fill

```text
gross edge (limit price vs reference)
- maker fee (required tier, bps on notional)
- slippage (0 for a limit filled at its own price — stated, not hidden)
= net edge
```

Spread, market impact, latency, and funding are **not** in v1 — each gets its
own row when measured, never a silent constant. A result that ignores them
says so on the chart (see the epistemic rules).

## Output

Per order: filled or expired-unfilled, fill price, fee paid, net edge, the
print that caused the fill (or the stream end that expired it), and the full
assumption header (fee tier, model version `exec-v1`, data tier, reproduce
command). Optimistic variants (fill on quote touch) may exist only with an
explicit `UNREALISTIC` label in the output itself.

## Acceptance

- Unit tests on synthetic print sequences: through-print fills, touch-without-
  print does not fill, wrong-side print does not fill, gap voids, expiry without
  prints.
- Live check: run resting-limit probes over a real trade capture; every claimed
  fill must cite its through-print. No print, no fill, no exceptions.
- Shadow-mode error (predicted vs tradable, Gate 2) is out of scope for v1 —
  v1 only has to be honest, not yet calibrated.

## Open questions (deliberately undecided)

- Tier C queue-position modeling for maker orders (a research project, not a
  parameter).
- Market-order fills at next-print price plus taker fee.
- Partial fills and multi-print sweeping.
- Per-venue fee schedules beyond a single bps input.
- Whether this ships as its own crate (`astra-exec`) or a replay consumer —
  decided at implementation time, not here.
