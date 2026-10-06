# 72-hour soak runbook

Status: **procedure defined, soak not run.** This is the exact procedure the
first 72h run follows — machine choice, shakedown, supervision, judging, and
what happens when something dies mid-soak.

## Do not use your laptop

The recorder's load is trivial (one WebSocket per stream, zstd compression on
chunk roll every 2,000 records) — the laptop is in no danger. The problem is
the lid: sleep kills connections, Wi-Fi drops, and a 72h desktop run that
pauses at hour 30 is not a soak, it is a story. Run the soak on a cheap VPS
that does not sleep:

- Any 2 vCPU / 4 GB box with **40 GB+ free disk** (Hetzner CX22/CX32 class,
  ~€4–6/mo; any equivalent droplet works). The strategy docs already budget
  $30/mo for collection — this fits inside it.
- Disk math, stated so it can be checked: Binance `book_diff` arrives at
  ~10/s with ~0.5–3 KB frames → worst case ~2.5 GB/day/stream raw, zstd
  compresses repetitive JSON ~5–10x → ~0.3 GB/day/stream. Four streams ×
  three days lands around **2–4 GB total**. Budget 10 GB; 40 GB is comfort.
- Region matters more than size: pick a region where the venue futures/spot
  endpoints are **not geo-blocked and not TLS-intercepted** (the README
  records a Fortinet interception that broke rustls outright). The shakedown
  below proves the route before the clock starts.

## Shakedown first (1–6 hours, same box)

Never start the 72h clock on an unproven machine. On the VPS, from a clean
checkout at a recorded commit:

```sh
git rev-parse HEAD                      # record the commit
cargo build -p astra-record
```

Run one short capture per stream (10–30 min is enough) and audit each:

```sh
./target/debug/astra-record capture --output ./shake-binance-btc \
  --venue binance --market spot --symbol BTC/USDT --channel book_diff \
  --duration-secs 600
./target/debug/astra-record check --input ./shake-binance-btc
# expect: verdict     healthy
```

The shakedown passes when every stream connects, frames arrive near venue
rate, `check` is `healthy`, and `git rev-parse HEAD` is recorded. Fix
firewall/TLS/geo issues here, not at hour 40. Delete the shakedown captures
after — they are not the soak.

## Start the soak

```sh
ops/soak.sh start
```

This builds, then launches four streams under `nohup` (Binance spot BTC+ETH,
Bybit spot+perp BTC) for 259,200 seconds with generous reconnects. Record:

- the commit hash, the VPS provider/region/specs, and the start timestamp
- the PIDs printed per stream

Then walk away. `nohup` + ` disown`-style backgrounding survives SSH drops;
do not run it under a laptop-tethered `ssh` session without `nohup` (the
script already handles that).

## Supervise (cheaply, not constantly)

```sh
ops/soak.sh status    # RUNNING vs STOPPED per stream
ops/soak.sh check     # per-stream verdicts + soak_verdict; non-zero on issues
```

Check once or twice a day. A STOPPED stream mid-soak with a `duration_elapsed`
stop reason and a `healthy` check is data, not failure — note it and let the
rule below decide. Do not restart individual streams to "top up" the soak:
**a restarted soak is a new soak, not a continuation.**

## If the machine dies

The partial capture plus its `check` output is still evidence — copy both
off the box, record what happened, and restart from zero on a fresh (or
fixed) machine. The failed attempt goes in the README failures section with
its `check` output, not in the bin.

## Judge

After 72h, `ops/soak.sh check` must report `healthy` on all four streams
(zero sequence breaks, every chunk hash-verified, every gap recorded with its
window marked unreliable), and the `gap` lines must show fewer than one
unexplained gap per instrument-day. Pass flips ROADMAP Gate 1 "Capture is
lossless" and "Sequence gaps are handled" toward VERIFIED; fail records the
findings and the soak reruns from zero.
