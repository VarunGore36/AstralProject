# Failure policy: what each subcommand does with bad data

Status: **documents current behavior; it does not unify it.** The audit
found that the "same" corruption produces different outcomes per
subcommand. Each outcome below was read off the code, not asserted from
intent. Unifying them is open work; this table is the baseline it will be
judged against.

Legend: **abort** = command fails with an error · **finding** = reported,
verdict unhealthy · **skip** = counted past · **null-row** = kept as a
null-field Parquet row · **passthrough** = surfaced to the caller unchanged.

| Corruption | `capture` (write) | `check` | `reconstruct` | `verify`/`compare` | `normalize` | `replay` / `exec probe` |
|---|---|---|---|---|---|---|
| Chunk fails SHA-256 | n/a (writes it) | abort (`Integrity`) | abort | abort | abort | abort |
| Manifest count disagrees with chunks | n/a (writes it) | finding (`manifest_mismatch`) | abort (`ManifestMismatch`) | abort (`ManifestMismatch`) | abort | abort |
| Manifest declares a newer schema | n/a (writes current) | abort (`SchemaVersion`) | abort | abort | abort | abort |
| Undecodable gap-marker payload | n/a (writes valid ones) | finding (`undecodable_gaps`) | **skipped silently** (synthetic records bypass parsing) | **skipped silently** | abort | abort (`Malformed`) |
| Unparseable venue frame | stored verbatim, unchecked | skip (`unchecked`, stays healthy alone) | skip (`frames_without_a_book`) | skip, uncounted | null-row | skip (`skipped_unparseable`) |
| Empty trade bundle (zero prints) | stored verbatim, unchecked | skip (`unchecked`) | skip (`frames_without_a_book`) | skip, uncounted | **zero rows** (no events, nothing to placeholder) | skip (`skipped_unparseable`) |
| Update-ID discontinuity | gap record, stream continues | finding | book breaks until a new snapshot | rejected events counted | rows as-is (continuity is the capture layer's job) | gap event passthrough |
| Sequence break (`seq` jumps) | impossible (writer assigns `seq`) | finding | **counted** (`seq_breaks`) | **not checked** | duplicate `seq` rejected | **not checked** (trusts order) |
| Duplicate update spans | gap record (strict equality) | **not detected** (forward jumps only) | applied twice, harmlessly (set semantics) | applied twice | accepted if `seq` differs | emitted twice |
| Half-written capture (`in_progress`, stale counts) | n/a | finding via count mismatch | abort via count mismatch | proceeds on whatever frames exist | abort via count mismatch | abort via count mismatch |
| Final disconnect, no trailing gap | stop reason only (correct: nothing follows) | nothing to find (read the manifest) | nothing to reconstruct past | n/a | n/a | n/a |

## Rules for changing this table

1. Unify toward fail-loud for corruption (abort or unhealthy), never toward
   silent skipping. The `reconstruct` synthetic-skip is the divergence that
   most deserves removal next.
2. The null-row vs skip divergence (normalize keeps, replay skips) stands
   until normalized-replay exists to need one answer.
3. Any behavior change here updates this table, the subcommand's golden
   tests, and the README verification record together.
