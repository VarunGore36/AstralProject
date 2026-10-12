# Benchmark corpus (seed)

The ghost-town antidote starts here: a tiny runnable corpus so a new user
can reproduce a result in under a minute, before recording anything.

## Quality tier: bronze

Stated plainly: this is a **synthetic assembly of real-format venue
frames** — 8 real Binance depth frames plus 2 real-format trade prints
(one verbatim from `testdata`, one price-shifted copy), packed into 3
chunks with a deterministic manifest. It proves the pipeline runs; it
proves nothing about any market. Research-grade tiers (silver: single
live source, gold: double-sourced and checksummed) arrive with real
collection, and every future addition names its tier here.

## Use it

```sh
# Reproduce the recorded run (verdict must read `reproduced`):
cargo run -q -p astra-harness -- verify \
  --registry benchmark-corpus/registry \
  --hash cb050f8f7f0384baa45bd434237da76f0e4b1a76d1fc144e95e9abcea34f78f7 \
  --input benchmark-corpus/capture

# List recorded runs:
cargo run -q -p astra-harness -- list --registry benchmark-corpus/registry

# Re-run the benchmark yourself (new report must hash identically):
cargo run -q -p astra-harness -- run \
  --input benchmark-corpus/capture \
  --config benchmark-corpus/config-v1.json \
  --output /tmp/seed-report.json
```

## What the seed runs contain

`config-v1.json`: seed 7, `book-top` strategy, two probes (buy 0.01 @
86000, sell 0.01 @ 85900, 10 bps maker). Both fill on the first trade
print — the seed demonstrates fills citing through-prints, not edge.

`config-v2.json`: seed 99, `trade-tally` strategy, the same two probes
plus `markouts_ns` horizons — demonstrating the markout section inside a
recorded run (benign/adverse signs, `null` past the tape end).

## Growing this corpus

Contributed captures graduate tiers only with provenance: source venue,
collection window, `check` verdict, and collection command recorded
alongside. Unvalidated contributions stay out of the `gold` tier by rule
(see the rethink on data poisoning). The recorder's future `--contribute`
flag will target this directory's layout.
