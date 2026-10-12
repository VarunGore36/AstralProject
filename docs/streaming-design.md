# Streaming replay and normalization (design + build notes)

Status: **both stream chunk-by-chunk (implemented — see "As built" notes
and the acceptance record).** Audit finding 1: only `check` streamed
chunk-by-chunk; replay and normalize `read_all`ed the whole capture,
needing single-digit GB for a full soak on the measured curve (~45 MB per
100k frames replay, ~115 MB normalize, debug builds). This document fixed
the design before anyone wrote code against it, so the determinism
guarantees survive the refactor.

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

### As built (replay, 2026-10-11)

Single streaming pass: each chunk is hash-verified, count-checked against
its index entry (the doctored-index guarantee the old path had), and fed to
the strategy before the next chunk is read. `seq` breaks abort as
`Malformed`, like a torn manifest — this caught a real spliced capture
during measurement (a resumed writer colliding `seq`, refused loud instead
of replaying 600k frames of two overlapping streams). The manifest count is
verified at end-of-stream: a torn manifest aborts with no report, though
the strategy may have observed events first — exactly like any mid-stream
abort (undecodable gap markers always behaved this way). A single pass was
chosen over a count-then-feed double pass after measurement showed the
double pass decoding every chunk twice for no behavioral gain.

Measured on a 300k-frame synthetic capture (debug build, peak child RSS):
trade-tally 141 MB → 12 MB (~12×); book-top 141 MB → 96 MB in 3 large
chunks. In 151 small chunks book-top peaks higher (~275 MB) from
debug-allocator churn across 151 alloc/free cycles, not from live data —
live inputs are one chunk in all cases. Per-event-signal strategies also
accumulate their own output (signals are the hashed result, O(events) by
design); eliminating *input* buffering is what this port guarantees, and
trade-tally proves it end to end.

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

### As built (normalize, 2026-10-11)

Option (a), with two corrections the design anticipated but did not
settle. Each chunk's rows are grouped by partition exactly like the old
whole-batch grouping, and each group is appended as a new row group to its
partition's staging writer (one `ArrowWriter` per partition, held open).
Staging files carry a `.tmp` suffix — invisible to the `part-*.parquet`
numbering — and are renamed into place only after the whole stream passes
the old error precedence (manifest, then first decode failure in stream
order, then per-table first failures in table order). A failed run deletes
its staging files and leaves no parquet behind.

Two things the design missed: `ArrowWriter` buffers rows until a row-group
limit (~1M rows) or `close`, so every chunk-segment write is followed by an
explicit `flush()` — without it the whole capture sat in memory and the
port was 5× *worse* than load-all (measured 1.1 GB before the flush,
28 MB after). And the byte-identity restatement is narrower than feared:
the same capture twice is still byte-identical (same chunks, same row
groups — the normalize-twice test passes unmodified), and the
cross-machine procedure needs no change (captures transfer with their
chunking). Only the same *records* under *different chunking* now compare
at read-back level, proven by the `chunking_changes_packaging_not_rows`
cross test — which also asserts no `.tmp` file ever escapes.

Measured on a 200k-frame synthetic capture (debug build, peak child RSS):
256 MB load-all → 28 MB streamed (~9×), identical file layout, identical
11 MB total output.

## Explicitly out of scope

- Changing what validation enforces (only how it is computed).
- Parallel chunk processing (deterministic merge or nothing — nothing, for now).
- Touching `check` (already streams) or the store format (append-only,
  content-addressed, and fine).

### Follow-up (2026-10-12): `reconstruct` and `compare` stream too

The same hazard lived in `reconstruct` and `compare` (both `read_all`ed).
Both now walk chunks through `store::stream_verified_records` — hash plus
count checked per chunk, O(chunk) memory, identical order and identical
error mapping. Proven by `chunking_does_not_change_reconstruction` (8 real
frames as 1 vs 8 chunks: identical book, identical counters). Every reader
in the pipeline now streams; `read_all` remains for tests and small tools.

## Acceptance

- [x] Replay: all determinism tests pass unmodified, plus a multi-chunk
  cross test (1/2/100 records-per-chunk over a mixed seven-frame capture
  with hand-computed expectations) and seq-break / torn-manifest aborts.
- [x] Normalize: all determinism tests pass unmodified (including the
  byte-identity twice test), plus the read-back cross test
  (`chunking_changes_packaging_not_rows`: 1 vs 100 records-per-chunk, same
  files, same logical rows, no staging leaks).
- [x] Peak RSS: replay input buffering eliminated (trade-tally ~12×, book-top
  bounded by its own signal output — see "As built"); normalize 256 MB →
  28 MB on a 200k-frame synthetic capture (~9×, identical layout and bytes
  out). Both measured as peak child RSS, debug builds.
- The README determinism rows and the cross-machine procedure state exactly
  which equality holds (bytes vs read-back) — no silent redefinition.
  (Same capture twice: byte-level, both paths. Same records under different
  chunking: replay byte-level, normalize read-back level. The cross-machine
  procedure transfers whole captures, so its byte-level claim stands.)
