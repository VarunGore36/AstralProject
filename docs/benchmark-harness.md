# Benchmark harness v1 (`astra-harness`)

Status: **specification; implementation follows in the same series.**
The missing keystone: everything downstream (registry entries, research
notes, external reproductions) points at the artifact defined here.

## What it is

One command turning a capture plus a config into a **hashed report**:

```sh
astra-harness run --input ./capture --config ./bench.json --output ./report.json
```

```text
input       ./capture
config      ./bench.json
probes      3
fills       1
report      ./report.json
report_hash 9f2c…
```

Same capture bytes + same config + same code → byte-identical `report.json`.
That file *is* the reproducible result: anyone with the capture can re-run
the command and compare one hash.

## Config (JSON, versioned)

```json
{
  "version": 1,
  "seed": 7,
  "strategy": "book-top",
  "probes": [
    { "side": "buy", "price": "100.00000000", "quantity": "1", "fee_bps": 5 }
  ]
}
```

- `strategy` names the replay strategy whose signals join the report.
  v1 supports `book-top` only; unknown names are refused, never defaulted.
- Every probe needs an explicit `fee_bps` (same rule as `exec-v1`).
- Unknown JSON fields are refused (fail-closed parsing): a config that
  means something different than intended must error, not run.

## Report (JSON, canonical)

```json
{
  "harness_version": "bench-v1",
  "exec_version": "exec-v1",
  "capture_id": "…",
  "frames": 601,
  "seed": 7,
  "strategy": "book-top",
  "signal_hash": "…",
  "probes": [
    {
      "side": "buy", "price": "100.00000000", "quantity": "1", "fee_bps": 5,
      "result": "filled", "fill_price": "100.00000000",
      "fee": "0.05000000", "print_seq": 41
    }
  ]
}
```

Canonical means byte-stable: struct field order is the file order, no hash
maps anywhere in the path, no timestamps, no wall-clock reads. The report
hash is SHA-256 over the exact bytes written (pretty JSON plus one trailing
newline). Unfilled probes report `"result": "unfilled"` with a `reason` field and the
fill fields absent, not null.

## Rules

- The capture is replayed **once**; all probes simulate over the collected
  stream. N probes must never mean N replays.
- A torn manifest, a future schema, or an undecodable gap aborts the run
  through the replay layer — the harness adds no second opinion.
- A gap voids every still-live probe (inherited from `exec-v1`).
- The harness never invents data: book signals come from replay, fills from
  exec, counts from the manifest. Anything else is a bug.
- Git commit is recorded *alongside* the report by the operator (runbook),
  not inside it: binaries cannot reliably know their own commit.

## Registry (local experiment log)

```sh
astra-harness run --input ./capture --config ./bench.json --output ./report.json --record ./experiments
astra-harness list --registry ./experiments
astra-harness verify --registry ./experiments --hash <report_hash> --input ./capture
# verdict     reproduced
```

`record` stores the report plus its config under
`<registry>/<report_hash>/` — content-addressed, so re-recording is a
no-op. `list` shows every run (hash, probes, fills, seed, capture).
`verify` re-runs the stored config against a capture directory and compares
hashes: exact match or `MISMATCH`, nothing in between. A mismatch exits
non-zero, so scripts judge it mechanically; unknown hashes fail loudly;
an empty directory lists as empty, not as an error.

## Acceptance

- Two runs over one capture + config → byte-identical files (asserted in CI
  on a fixture capture).
- Tampered manifest → clean refusal, no report written.
- Golden test on the report shape (keys present, unfilled shape exact).
- Live check when venue routes allow: probes over a real trade capture,
  every fill citing its print.

## Open questions (deliberately undecided)

- More strategies (needs the strategy SDK question answered first).
- CSV/human summaries next to the canonical JSON (never instead of it).
- Whether `strategy` stays a name or becomes a WASM/plugin boundary.
