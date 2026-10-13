#!/usr/bin/env python3
"""Research note 005 analysis: funding carry in a hostile regime, 2022-Q4.

Same unconditional hold as note 004 (long $10k spot + short $10k perp, all
of each quarter's first three months) run against the Oct-Nov-Dec 2022 bear
market, when funding averaged near zero and went negative in November (FTX).
Money math in Decimal (exact); floats only for the labeled Sharpe analytic.

Usage:
    python3 research/scripts/carry_005.py --data-dir /tmp/carry005
Downloads the pinned archive slices (verifies SHA-256 first), analyzes,
prints the result tables pasted into research/005-hostile-carry.md.
Stdlib only. Independent of carry_004.py by design: each note's script
reproduces its own note from scratch.
"""

import argparse
import csv
import datetime
import hashlib
import io
import pathlib
import urllib.request
import zipfile
from decimal import Decimal

NOTIONAL = Decimal("10000")
FEE_BPS = Decimal("5")
FEE_PER_FILL = NOTIONAL * FEE_BPS / Decimal("10000")
MONTHS = ["2022-10", "2022-11", "2022-12"]

FILES = {
    "funding": (
        "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-{m}.zip",
        "ad9efed10d4ca1567b865d7a158790977cec1b925395634217069df6696c8bc9",
        "8febe5bec1a029e993bbf27490761f8c47c32849bf7dd13441b2b2f6ec1ffb51",
        "4218c78331bcc4dbeb5768fa112242a880971c1f2cfd1d3f32aaa26ca34069af",
    ),
    "spot": (
        "data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-{m}.zip",
        "2591cbbe022ce9d4559aee1fa41ea0033324c2c4debc776cfedc9588167d413b",
        "5608adf8602a8ac616ee03c2c4461a37f4646631d67edf3d2f1a94e82e5c5065",
        "69c5e5f01df947bfea48ab3c8e2f462b7358effa9e2700489a4ad0dbd29b24aa",
    ),
    "perp": (
        "data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-{m}.zip",
        "d85a5f4bdbe93df2babdecf3b37f866655fa23c3fb31eeca96c3e3f6a1e9b872",
        "e9b3f3b1929e3f6f255269c2d2eb564d9d9bcacbfc16651888e95765df383888",
        "d313c2cb509cba13eff6538829245aedc993f18eff4a7bd48fe0eb6c3a9fa158",
    ),
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


def month_of(ts_ms: int) -> str:
    return datetime.datetime.fromtimestamp(
        ts_ms / 1000, tz=datetime.timezone.utc
    ).strftime("%Y-%m")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    paths = fetch(pathlib.Path(args.data_dir))

    funding = [row for path in paths["funding"] for row in read_funding(path)]
    spot = [row for path in paths["spot"] for row in read_klines(path)]
    perp = [row for path in paths["perp"] for row in read_klines(path)]
    misaligned = sum(1 for a, b in zip(spot, perp) if a[0] != b[0])
    print(f"payments={len(funding)} spot_bars={len(spot)} perp_bars={len(perp)} misaligned_hours={misaligned}")

    received = sum(rate * NOTIONAL for _, rate in funding)
    fees = 4 * FEE_PER_FILL

    entry_basis = perp[0][1] - spot[0][1]
    exit_basis = perp[-1][1] - spot[-1][1]
    drift = exit_basis - entry_basis
    size = NOTIONAL / spot[0][1]
    basis_pnl = -drift * size
    net = received - fees + basis_pnl

    by_month: dict = {}
    neg_months: dict = {}
    for ts, rate in funding:
        month = month_of(ts)
        by_month[month] = by_month.get(month, Decimal("0")) + rate * NOTIONAL
        neg_months[month] = neg_months.get(month, 0) + (1 if rate < 0 else 0)
    for month in MONTHS:
        total = len([1 for ts, _ in funding if month_of(ts) == month])
        print(f"funding {month}: {by_month.get(month, Decimal('0')):.2f} "
              f"({neg_months.get(month, 0)}/{total} payments negative)")

    avg_rate_bps = sum(rate for _, rate in funding) / len(funding) * Decimal("10000")
    print(f"payments={len(funding)} avg_rate_bps={avg_rate_bps:.2f}")
    print(f"spot_entry={spot[0][1]} spot_exit={spot[-1][1]}")
    print(f"perp_entry={perp[0][1]} perp_exit={perp[-1][1]}")
    print(f"basis_entry={entry_basis:.2f} basis_exit={exit_basis:.2f}")
    print(f"funding_received={received:.2f} fees={fees:.2f} basis_pnl={basis_pnl:.2f}")
    print(f"net={net:.2f} on 20000 capital = {(net / Decimal('20000') * 100):.2f}%")

    days: dict = {}
    for (ts, s), (_, p) in zip(spot, perp):
        days[ts // 86_400_000] = (s, p)
    ordered = sorted(days)
    funding_by_day: dict = {}
    for ts, rate in funding:
        funding_by_day[ts // 86_400_000] = (
            funding_by_day.get(ts // 86_400_000, Decimal("0")) + rate * NOTIONAL
        )
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
    import statistics
    mean = statistics.fmean(rets)
    sd = statistics.pstdev(rets)
    print(f"trading_days={len(rets)} daily_sharpe={(mean / sd * (365 ** 0.5)) if sd else 0:.2f} (analytic floats)")


if __name__ == "__main__":
    main()
