# Research note 015: simultaneity doesn't matter for v1 fills (proven, not asserted)

Date: 2026-10-16 · Status: **engine property, proven by construction plus
4,096 adversarial streams.** Closes the simultaneity question notes 003
and 009 deferred: roughly half of all prints share their millisecond with
a neighbor, and replay order inside the millisecond is arrival order —
venue-opaque, not execution order. Does that phantom precision change any
fill?

## Question

`exec-v1` fills a resting limit order on the first trade print trading
through its price. Inside a shared millisecond, "first" is arrival order —
knowledge the engine cannot have. If the orderings disagree on fills, the
model's audit trail cites evidence it never possessed.

## Method

`simulate_batched` (new in `astra-exec`, same rules, same errors):
consecutive prints sharing a venue millisecond evaluate as one
simultaneous batch — fills iff any sound print in the batch trades
through, citing the earliest such print. Prints without venue time form
singleton batches; gaps flush then void, exactly as in streaming. Then the
equivalence proof: directed torture shapes (through-print last in batch,
corrupt prints leading, timeless prints interleaved, gaps splitting
milliseconds, both sides) plus 4,096 seeded streams mixing all of it —
full outcome equality (verdict, price, fee, cited print) on every one.

## Result

Identical on all 4,096 streams and all torture shapes: batching can cite
a different print in theory, but the first through-print in stream order
is always the earliest through-print of its batch, so verdict, price, fee,
and cited print all match `simulate` exactly. The phantom precision was
phantom: arrival order never changes what v1 concludes, only what it
would have cited had the batch disagreed — and it never does.

## Reading

One fact with a boundary. The fill/no-fill question is order-invariant
within the millisecond because v1 fills whole orders at the limit on first
evidence — existence of a through-print does not depend on order. Anything
finer (which print filled how much of the order, queue position inside
the batch) is order-dependent, and that is exactly Tier C: the batching
machinery now exists, tested, waiting for the model that needs it. Until
then the cited print stays arrival-first and says so, and no result
depends on it.

## Reproduce

```sh
cargo test -p astra-exec batching
```

Two tests: directed torture shapes plus the seeded sweep. No archive, no
venue — pure engine property, runnable offline.
