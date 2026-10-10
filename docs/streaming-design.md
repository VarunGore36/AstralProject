# Streaming replay and normalization (design)

Status: **specification only, not implemented.** Audit finding 1: only
`check` streams chunk-by-chunk; replay and normalize `read_all` the whole
capture, needing single-digit GB for a full soak on the measured curve
(~45 MB per 100k frames replay, ~115 MB normalize, debug builds). This
document fixes the design before anyone writes code against it, so the
determinism guarantees survive the refactor.

## Non-negotiables

- Byte-identical outputs to the load-all paths on the same inputs. The
  existing determinism tests (normalize-twice, ten replays) must pass
  unmodified against the streaming implementations, plus a new cross test:
  stream(normalize(X)) == bytes of load-all(normalize(X)) on a multi-chunk
  capture.
- No wall-clock reads, no hash-map iteration in event order, no
  order-dependent accumulation — the current determinism rules, unchanged.
- Validation stays whole-batch in *effect*: duplicate keys, range violations,
  and manifest mismatches must still reject (or report) exactly as today,
  even when the offending row arrives in chunk 40 of 150.

## Shape of the solution (replay)

Replay already processes records in order; only acquisition changes:

```text
for each chunk in index order:
    verify hash, decode records
    feed events to the strategy in seq order
```

`seq` continuity across chunk boundaries must be enforced (or at least
surfaced — decide during implementation, default to erroring like a torn
manifest). Gap semantics, ordering rules, and the virtual clock are
untouched. Memory becomes O(largest chunk), not O(capture).

## Shape of the solution (normalize)

Harder, because outputs group by (instrument, date) while inputs stream by
time. Two-pass design:

- Pass 1 (streaming read): parse and validate rows chunk by chunk, keeping
  only running validation state (seen `(capture_id, seq, print_index)` key
  hashes for duplicate detection, min/max sanity accumulators — all
  order-independent).
- Pass 2 (grouped write): spill validated rows per partition. Options, in
  preference order: (a) one ArrowWriter per partition held open across
  chunks (Parquet writers support repeated `write` calls before `close`;
  verify byte-equality against single-shot output — row-group boundaries
  will differ, so equality must be defined at the *read-back* level, not
  the byte level, and the determinism claim restated accordingly); (b)
  spill row groups to temp files, then merge. (a) is preferred if the
  read-back-equality proof holds; the byte-identity claim for normalization
  would then narrow to same-partition-single-shot, which must be stated
  loudly wherever the old claim appears (README, cross-machine procedure).

## Explicitly out of scope

- Changing what validation enforces (only how it is computed).
- Parallel chunk processing (deterministic merge or nothing — nothing, for now).
- Touching `check` (already streams) or the store format (append-only,
  content-addressed, and fine).

## Acceptance

- All current determinism tests pass unmodified, plus the cross test above
  on a multi-chunk capture.
- Peak RSS on the 300k-frame synthetic capture drops by an order of
  magnitude for both paths (measure with the same VmHWM method).
- The README determinism rows and the cross-machine procedure state exactly
  which equality holds (bytes vs read-back) — no silent redefinition.
