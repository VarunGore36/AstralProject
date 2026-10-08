# Cross-machine reproduction procedure

Status: **procedure defined, execution open.** No second-machine run has
happened yet. This document exists so that when it does, the result is a
pass/fail comparison of hashes — not a vibe check.

## What must match exactly

Given the same capture bytes, the same code, and the same seed:

1. `astra-record check` prints `verdict     healthy` on both machines.
2. The normalized Parquet file tree hashes identically on both machines.
3. `astra-replay` prints the identical `signal_hash` on both machines.

If any of the three differs, the determinism claim fails. There is no
"close enough" — hashes are equal or the run is red.

## Why this should hold (mechanism, not hope)

- `ts_ready` is always written null (`normalize.rs` appends null in all three
  writers), so no wall-clock time leaks into normalized output.
- Replay strategies observe virtual time only; the engine exposes a seeded RNG
  and no clock (`replay.rs` contains no `SystemTime`, `Instant`, threads, or
  hash-map iteration in the event path).
- Row order within a date partition follows capture order; date partitions are
  sorted. File hashing therefore compares content, not filesystem order.

## Procedure

On machine A, from a clean checkout:

```sh
git rev-parse HEAD                      # code hash, record it
cargo build --workspace

astra-record check --input ./capture    # must print: verdict     healthy

astra-normalize --input ./capture --output ./norm-a
find ./norm-a -name '*.parquet' -exec sha256sum {} + | awk '{print $1}' | sort | sha256sum
# ^ dataset hash, record it (contents only: raw sha256sum output embeds
# file paths, so comparing it across differently-named directories always
# mismatches even on identical bytes)

astra-replay --input ./capture --seed 7 --strategy book-top
# ^ signal_hash, record it (repeat with --seed 99: book-top must agree)
```

Transfer to machine B: the **entire `./capture` directory byte-for-byte**
(rsync with checksums, not a re-download) plus the recorded code hash.
Machine B checks out that exact commit and runs:

```sh
git rev-parse HEAD                      # must equal machine A's hash
cargo build --workspace

astra-record check --input ./capture    # must print: verdict     healthy

astra-normalize --input ./capture --output ./norm-b
find ./norm-b -name '*.parquet' -exec sha256sum {} + | awk '{print $1}' | sort | sha256sum
# ^ must equal machine A's dataset hash (same content-only recipe)

astra-replay --input ./capture --seed 7 --strategy book-top
# ^ signal_hash must equal machine A's signal_hash
```

## Recording the result

A passing run reports four equalities: code hash, `healthy` verdicts,
dataset hashes, signal hashes — plus the capture's own identity
(`capture_id` from the manifest) and the seed. A failing run reports which
equality broke first and keeps both outputs for diffing. Either outcome goes
in the README verification record; a pass flips ROADMAP Gate 1
"Cross machine reproducibility" from OPEN to VERIFIED.

## Known limits

- Toolchain differences (rustc version, Arrow/Parquet versions via
  `Cargo.lock`) are pinned by the lockfile but not yet tested across OSes.
  Same-OS, same-commit is the bar for the first run; cross-OS comes after.
- This covers `book_diff` + `trade` + `top_of_book` replay and normalization.
  `funding` / `liquidation` have no verified live payloads, so they are out
  of scope until they do.
