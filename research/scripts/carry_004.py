#!/usr/bin/env python3
"""Research note 004 analysis: delta-neutral BTC funding carry, 2024-01..03.

Long $10k BTC spot + short $10k BTC perp held the whole window. PnL =
funding received - entry/exit fees - basis drift. Money math in
Decimal (exact); floats only for the labeled Sharpe analytic.

Usage:
    python3 research/scripts/carry_004.py --data-dir /tmp/carry004
Downloads the pinned archive slices (verifies SHA-256 first), analyzes,
prints the result tables pasted into research/004-funding-carry.md.
Stdlib only.
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
MONTHS = ["2024-01", "2024-02", "2024-03"]

FILES = {
    # url suffix, local name, sha256 (pinned 2026-10-12; re-pin on change)
    "funding": (
        "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-{m}.zip",
         "3e0d30870672aa8f0f937881056e3cfd55913ae5c780cd50b33f2763aa0ba58e",
         "daf1e4901e3d6436d82ad60f90d007eb5300be8047cfef121007203a7212cde6",
         "711dcaf2a341aedfd06447b4117b540f60adb0784c5e64a313c718e4cf092bc3"),
    "spot": (
        "data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-{m}.zip",
         "cf873a185bd5b24b8e00034e49583fcb49928e0c3a45c6fc27a632a683655417",
         "b83aa7319ef1d4baa7b923c0fe802b88dfaf241c7456af295f1f656a24379b33",
         "b0851daf609d6ab82fdf8f1d1c4fbd974bc1b4e6530a832067516ac1c2be63a6"),
    "perp": (
        "data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-{m}.zip",
         "bf673f3d10804a951e8bac56dd2473486f113025971d43ebe5258ec40f9bfeb3",
         "655ff5ac0fc570956756944c7afef4895c9bd149342f1e437de97292cbd1fe22",
         "8138642b757ddf5a42207130277be4569afb4c581a0a1817be9f5f35a4415498"),
}
BASE = "https://data.binance.vision/"


def fetch(data_dir: pathlib.Path) -> dict:
    data_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    for kind, (template, *hashes) in FILES.items():
        paths[kind] = []
        for month, want in zip(MONTHS, hashes):
            url = BASE + template.format(m=month)
            local = data_dir / url.split("/")[-1].replace(".zip", f".{kind}.zip")
            if not local.exists():
                urllib.request.urlretrieve(url, local)
            digest = hashlib.sha256(local.read_bytes()).hexdigest()
            assert digest == want, f"{local}: {digest} != pinned {want}"
            paths[kind].append(local)
    return paths


def read_funding(path: pathlib.Path):
    with zipfile.ZipFile(path) as archive:
        text = archive.read(archive.namelist()[0]).decode()
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        rows.append((int(row["calc_time"]), Decimal(row["last_funding_rate"])))
    return rows


def read_klines(path: pathlib.Path):
    with zipfile.ZipFile(path) as archive:
        text = archive.read(archive.namelist()[0]).decode()
    rows = []
    for row in csv.reader(io.StringIO(text)):
        if row[0] == "open_time":
            continue
        rows.append((int(row[0]), Decimal(row[4])))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    paths = fetch(pathlib.Path(args.data_dir))

    funding = [row for path in paths["funding"] for row in read_funding(path)]
    spot = [row for path in paths["spot"] for row in read_klines(path)]
    perp = [row for path in paths["perp"] for row in read_klines(path)]
    print(f"payments={len(funding)} spot_bars={len(spot)} perp_bars={len(perp)}")

    # Short perp receives funding when the rate is positive.
    received = sum(rate * NOTIONAL for _, rate in funding)
    fees = 4 * FEE_PER_FILL

    # Basis drift over the hold: long spot + short perp gains when the
    # premium compresses. Entry/exit at first/last hourly closes.
    entry_basis = perp[0][1] - spot[0][1]
    exit_basis = perp[-1][1] - spot[-1][1]
    drift = exit_basis - entry_basis  # per-1.0-size; scale by size below
    size = NOTIONAL / spot[0][1]
    basis_pnl = -drift * size
    net = received - fees + basis_pnl

    # Monthly split of funding received.
    by_month: dict = {}
    for ts, rate in funding:
        month = "2024-01" if ts < 1706745600000 else ("2024-02" if ts < 1709251200000 else "2024-03")
        by_month[month] = by_month.get(month, Decimal("0")) + rate * NOTIONAL
    for month in MONTHS:
        print(f"funding {month}: {by_month.get(month, Decimal('0')):.2f}")

    avg_rate_bps = sum(rate for _, rate in funding) / len(funding) * Decimal("10000")
    print(f"payments={len(funding)} avg_rate_bps={avg_rate_bps:.2f}")
    print(f"spot_entry={spot[0][1]} spot_exit={spot[-1][1]}")
    print(f"perp_entry={perp[0][1]} perp_exit={perp[-1][1]}")
    print(f"basis_entry={entry_basis:.2f} basis_exit={exit_basis:.2f}")
    print(f"funding_received={received:.2f} fees={fees:.2f} basis_pnl={basis_pnl:.2f}")
    print(f"net={net:.2f} on 20000 capital = {(net / Decimal('20000') * 100):.2f}%")

    # Daily mark-to-market for a labeled Sharpe analytic (floats OK here).
    days: dict = {}
    for (ts, s), (_, p) in zip(spot, perp):
        day = ts // 86_400_000
        days[day] = (s, p)
    ordered = sorted(days)
    equity = []
    funding_by_day: dict = {}
    for ts, rate in funding:
        funding_by_day[ts // 86_400_000] = funding_by_day.get(ts // 86_400_000, Decimal("0")) + rate * NOTIONAL
    running = Decimal("0")
    prev = None
    rets = []
    s0, p0 = days[ordered[0]]
    for day in ordered:
        s, p = days[day]
        running += funding_by_day.get(day, Decimal("0"))
        value = (s - s0) * size - (p - p0) * size + running
        if prev is not None and prev != 0:
            rets.append(float((value - prev) / Decimal("20000")))
        prev = value
        equity.append(value)
    import statistics
    mean = statistics.fmean(rets)
    sd = statistics.pstdev(rets)
    print(f"trading_days={len(rets)} daily_sharpe={(mean / sd * (365 ** 0.5)) if sd else 0:.2f} (analytic floats)")


if __name__ == "__main__":
    main()
