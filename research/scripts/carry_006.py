#!/usr/bin/env python3
"""Research note 006 analysis: basis-timed entries for the carry trade.

Both carry notes defer this: does entering only when the perp premium is
historically rich buy back the dead quarters? Unconditional hold versus a
hysteresis rule — enter when the premium sits at/above the 75th percentile
of its trailing history, exit when it falls below the median — over the
combined Oct-2022..Mar-2023 window both earlier notes already pinned.

Anti-look-ahead rule, enforced in code: percentiles use strictly trailing
data (expanding window, 720h minimum warmup — no trading before it). Money
in Decimal (exact); floats only for labeled analytics. Funding is credited
when the position is open at the payment timestamp. Fills at hourly closes,
5 bps taker, $10k legs — the same simplifications as notes 004/005, so the
comparison is clean: only the entry rule changed.

Usage:
    python3 research/scripts/carry_006.py --data-dir /tmp/carry006
Stdlib only. Independent script per note; pins transcribed from the
SHA-verified 004/005 runs and re-enforced here.
"""

import argparse
import csv
import hashlib
import io
import pathlib
import urllib.request
import zipfile
from decimal import Decimal

NOTIONAL = Decimal("10000")
FEE_BPS = Decimal("5")
FEE_PER_FILL = NOTIONAL * FEE_BPS / Decimal("10000")
WARMUP_HOURS = 720

# (url template, sha per month). Pins re-verified by this script on download.
FUNDING_MONTHS = ["2022-10", "2022-11", "2022-12", "2024-01", "2024-02", "2024-03"]
FUNDING_HASHES = [
    "ad9efed10d4ca1567b865d7a158790977cec1b925395634217069df6696c8bc9",
    "8febe5bec1a029e993bbf27490761f8c47c32849bf7dd13441b2b2f6ec1ffb51",
    "4218c78331bcc4dbeb5768fa112242a880971c1f2cfd1d3f32aaa26ca34069af",
    "3e0d30870672aa8f0f937881056e3cfd55913ae5c780cd50b33f2763aa0ba58e",
    "daf1e4901e3d6436d82ad60f90d007eb5300be8047cfef121007203a7212cde6",
    "711dcaf2a341aedfd06447b4117b540f60adb0784c5e64a313c718e4cf092bc3",
]
SPOT_HASHES = [
    "2591cbbe022ce9d4559aee1fa41ea0033324c2c4debc776cfedc9588167d413b",
    "5608adf8602a8ac616ee03c2c4461a37f4646631d67edf3d2f1a94e82e5c5065",
    "69c5e5f01df947bfea48ab3c8e2f462b7358effa9e2700489a4ad0dbd29b24aa",
    "cf873a185bd5b24b8e00034e49583fcb49928e0c3a45c6fc27a632a683655417",
    "b83aa7319ef1d4baa7b923c0fe802b88dfaf241c7456af295f1f656a24379b33",
    "b0851daf609d6ab82fdf8f1d1c4fbd974bc1b4e6530a832067516ac1c2be63a6",
]
PERP_HASHES = [
    "d85a5f4bdbe93df2babdecf3b37f866655fa23c3fb31eeca96c3e3f6a1e9b872",
    "e9b3f3b1929e3f6f255269c2d2eb564d9d9bcacbfc16651888e95765df383888",
    "d313c2cb509cba13eff6538829245aedc993f18eff4a7bd48fe0eb6c3a9fa158",
    "bf673f3d10804a951e8bac56dd2473486f113025971d43ebe5258ec40f9bfeb3",
    "655ff5ac0fc570956756944c7afef4895c9bd149342f1e437de97292cbd1fe22",
    "8138642b757ddf5a42207130277be4569afb4c581a0a1817be9f5f35a4415498",
]
BASE = "https://data.binance.vision/"


def fetch(data_dir: pathlib.Path):
    data_dir.mkdir(parents=True, exist_ok=True)

    def get(url: str, want: str, local_name: str) -> pathlib.Path:
        local = data_dir / local_name
        if not local.exists():
            urllib.request.urlretrieve(url, local)
        digest = hashlib.sha256(local.read_bytes()).hexdigest()
        assert digest == want, f"{local}: {digest} != pinned {want}"
        return local

    funding, spot, perp = [], [], []
    for month, fh, sh, ph in zip(FUNDING_MONTHS, FUNDING_HASHES, SPOT_HASHES, PERP_HASHES):
        funding.append(get(
            BASE + f"data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-{month}.zip", fh,
            f"funding-{month}.zip"))
        spot.append(get(
            BASE + f"data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-{month}.zip", sh,
            f"spot-{month}.zip"))
        perp.append(get(
            BASE + f"data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-{month}.zip", ph,
            f"perp-{month}.zip"))
    return funding, spot, perp


def read_funding(path: pathlib.Path):
    with zipfile.ZipFile(path) as archive:
        text = archive.read(archive.namelist()[0]).decode()
    return [(int(r["calc_time"]), Decimal(r["last_funding_rate"]))
            for r in csv.DictReader(io.StringIO(text))]


def read_klines(path: pathlib.Path):
    with zipfile.ZipFile(path) as archive:
        text = archive.read(archive.namelist()[0]).decode()
    return [(int(r[0]), Decimal(r[4])) for r in csv.reader(io.StringIO(text))
            if r[0] != "open_time"]


def percentile(sorted_vals, pct: int) -> Decimal:
    # Nearest-rank: the smallest value with at least pct% at or below it.
    import math
    rank = max(1, math.ceil(pct / 100 * len(sorted_vals)))
    return sorted_vals[rank - 1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--enter-pct", type=int, default=75)
    parser.add_argument("--exit-pct", type=int, default=50)
    args = parser.parse_args()
    if not 0 < args.exit_pct < args.enter_pct <= 100:
        parser.error("need 0 < --exit-pct < --enter-pct <= 100")
    entry_pct, exit_pct = args.enter_pct, args.exit_pct
    print(f"band: enter p{entry_pct}, exit p{exit_pct}")
    fund_paths, spot_paths, perp_paths = fetch(pathlib.Path(args.data_dir))

    funding = [r for p in fund_paths for r in read_funding(p)]
    spot = [r for p in spot_paths for r in read_klines(p)]
    perp = [r for p in perp_paths for r in read_klines(p)]
    # Monthly files arrive in order but months may gap (Oct-Dec then Jan-Mar):
    # sort by timestamp and require hour alignment before any math.
    spot.sort()
    perp.sort()
    funding.sort()
    misaligned = sum(1 for a, b in zip(spot, perp) if a[0] != b[0])
    print(f"payments={len(funding)} spot_bars={len(spot)} perp_bars={len(perp)} "
          f"misaligned_hours={misaligned} span={spot[0][0]}..{spot[-1][0]}")
    assert misaligned == 0 and len(spot) == len(perp)

    # The pinned months leave a nine-month hole (2023-01..09): a position
    # cannot meaningfully span it, and trailing percentiles must not jump
    # across it. Split into contiguous blocks (hourly grid, tolerance one
    # missing bar) and run each block as its own experiment.
    blocks: list = []
    start = 0
    for i in range(1, len(spot)):
        if spot[i][0] - spot[i - 1][0] > 2 * 3600000:
            blocks.append((start, i))
            start = i
    blocks.append((start, len(spot)))
    print(f"contiguous_blocks={len(blocks)} sizes={[end - s for s, end in blocks]}")
    assert len(blocks) == 2

    size = NOTIONAL / spot[blocks[0][0] + WARMUP_HOURS][1]

    def block_baseline(s: int, end: int):
        received = Decimal("0")
        for ts, rate in funding:
            if spot[s + WARMUP_HOURS][0] <= ts <= spot[end - 1][0]:
                received += rate * NOTIONAL
        drift = (perp[end - 1][1] - spot[end - 1][1]) - (
            perp[s + WARMUP_HOURS][1] - spot[s + WARMUP_HOURS][1])
        return received, -drift * size

    base_received = Decimal("0")
    base_drift_pnl = Decimal("0")
    for s, end in blocks:
        received, drift_pnl = block_baseline(s, end)
        base_received += received
        base_drift_pnl += drift_pnl
    base_net = base_received - 4 * FEE_PER_FILL * len(blocks) + base_drift_pnl
    print(f"baseline unconditional: funding={base_received:.2f} drift={base_drift_pnl:.2f} "
          f"fees={4 * FEE_PER_FILL * len(blocks):.2f} net={base_net:.2f}")

    # Timed rule with strictly trailing percentiles, per contiguous block
    # (same block discipline as the baseline: no state crosses the hole).
    # One state machine per block: spans recorded with entry basis (for
    # drift), funding attributed to payments inside open spans afterwards.
    # Open time counts bars held, never wall-clock across missing months.
    timed_drift = Decimal("0")
    trades = 0
    open_bars = 0
    tradeable_bars = 0
    entries: list = []
    all_spans: list = []
    for s, end in blocks:
        history: list = []
        holding = False
        start_ts = 0
        start_basis = Decimal("0")
        for index in range(s, end):
            ts, price_s = spot[index]
            _, price_p = perp[index]
            basis = price_p - price_s
            if index - s >= WARMUP_HOURS:
                tradeable_bars += 1
                ordered = sorted(history)
                if not holding and basis >= percentile(ordered, entry_pct):
                    holding = True
                    start_ts, start_basis = ts, basis
                    trades += 1
                    entries.append((ts, basis))
                elif holding and basis < percentile(ordered, exit_pct):
                    holding = False
                    all_spans.append((start_ts, ts))
                    timed_drift += -(basis - start_basis) * size
                    trades += 1
            history.append(basis)
            if holding:
                open_bars += 1
        if holding:
            all_spans.append((start_ts, spot[end - 1][0] + 3600000))
            _, plast = perp[end - 1]
            _, slast = spot[end - 1]
            timed_drift += -((plast - slast) - start_basis) * size
            trades += 1
    funding_got = Decimal("0")
    for ts, rate in funding:
        if any(open_ts <= ts < close_ts for open_ts, close_ts in all_spans):
            funding_got += rate * NOTIONAL

    timed_net = funding_got - trades * 2 * FEE_PER_FILL + timed_drift
    print(f"timed rule: round_trips~{trades // 2} fills={trades} "
          f"open_bars={open_bars}/{tradeable_bars} "
          f"funding={funding_got:.2f} drift={timed_drift:.2f} net={timed_net:.2f}")
    print(f"entries(first3)={[(t, f'{b:.2f}') for t, b in entries[:3]]}")


if __name__ == "__main__":
    main()
