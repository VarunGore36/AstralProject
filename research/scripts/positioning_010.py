#!/usr/bin/env python3
"""Research note 010 analysis: positioning through the 2023-03-24 halt.

Binance halted spot trading on 2023-03-24 (~11:00-13:00 UTC, matching-engine
bug) while perps kept trading. Seven days of 5-minute metrics (open
interest, account long/short ratios) around the halt, plus hourly closes
for price context: did positioning see it coming (OI flush?), and what did
it do while spot was dark (ratio whiplash?)? Money in Decimal; floats only
for labeled analytics. Stdlib only.

Usage:
    python3 research/scripts/positioning_010.py --data-dir /tmp/pos010
Downloads nine pinned archive slices (verifies SHA-256 first), analyzes,
prints the tables pasted into research/010-halt-positioning.md.
"""

import argparse
import csv
import hashlib
import io
import pathlib
import urllib.request
import zipfile
from decimal import Decimal

DAYS = ["2023-03-20", "2023-03-21", "2023-03-22", "2023-03-23",
        "2023-03-24", "2023-03-25", "2023-03-26"]
# Pinned SHA-256 of the downloaded zips. Metrics files are kilobytes, pulled
# whole; klines pins match carry_008's verified pins for the same files.
METRICS_HASHES = [
    "c0e15ae357f03ea71070e8f39512221ac1ed3b71123ab1ce5681a7f6f9cff2cc",
    "218fc1120c479969e554e1db51134eac19c86e025de4e8bf4ef47e6732362fb1",
    "f2a6dc409efd5daedb00a1b88709cbfffd325574b04505a9b5343eb11270ddf7",
    "8cd38e36e4f063e7ddbf6f73748e338bb664589b81bc33377c31647d435de81f",
    "9e021c0099ad26f194b9b12cdac8df3faca71a5724fe08f99ad1223177561d4f",
    "2ba8c58e7161d5962b0209c3c87ed87fad197044fe21a26acb42d7ded017bb58",
    "aad03bb549f072fbdcdca40f1b32346046d667554ddad431e0f149a023c6a799",
]
SPOT_SHA = "7f2afb8e0179a57ac31eab5205660298ba5eb77039ac2e21aef9b715ff3d06ce"
PERP_SHA = "35f0068cc41887ea11960c4bb10e251a404fef5ad4cb4c88699cc2419b6bc38b"
BASE = "https://data.binance.vision/"


def fetch(data_dir: pathlib.Path):
    data_dir.mkdir(parents=True, exist_ok=True)

    def get(url: str, want: str, local_name: str) -> pathlib.Path:
        local = data_dir / local_name
        if not local.exists():
            urllib.request.urlretrieve(url, local)
        digest = hashlib.sha256(local.read_bytes()).hexdigest()
        assert digest == want, f"{local_name}: {digest} != pinned {want}"
        return local

    metrics = []
    for day, want in zip(DAYS, METRICS_HASHES):
        metrics.append(get(
            BASE + f"data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-{day}.zip",
            want, f"metrics-{day}.zip"))
    spot = get(BASE + "data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2023-03.zip",
               SPOT_SHA, "spot-2023-03.zip")
    perp = get(BASE + "data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2023-03.zip",
               PERP_SHA, "perp-2023-03.zip")
    return metrics, spot, perp


def read_metrics(path: pathlib.Path):
    with zipfile.ZipFile(path) as archive:
        text = archive.read(archive.namelist()[0]).decode()
    return list(csv.DictReader(io.StringIO(text)))


def read_klines(path: pathlib.Path):
    with zipfile.ZipFile(path) as archive:
        text = archive.read(archive.namelist()[0]).decode()
    return [(int(r[0]), Decimal(r[4])) for r in csv.reader(io.StringIO(text))
            if r[0] != "open_time"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    metric_paths, spot_path, perp_path = fetch(pathlib.Path(args.data_dir))

    print("day: OI open->close (chg) | count L/S open->close min/max")
    for day, path in zip(DAYS, metric_paths):
        rows = read_metrics(path)
        assert len(rows) == 288, f"{day}: {len(rows)} rows != 288"
        oi = [Decimal(r["sum_open_interest"]) for r in rows]
        ls = [Decimal(r["count_long_short_ratio"]) for r in rows]
        print(f"{day}: OI {oi[0]:.0f}->{oi[-1]:.0f} ({oi[-1] - oi[0]:+.0f}) | "
              f"L/S {ls[0]:.2f}->{ls[-1]:.2f} {min(ls):.2f}/{max(ls):.2f}")

    # Halt-day intraday path, hourly samples.
    rows = read_metrics(metric_paths[4])
    print("2023-03-24 hourly (OI, count L/S, top-trader L/S):")
    for row in rows[::12]:
        print(f"  {row['create_time'][11:16]} OI {Decimal(row['sum_open_interest']):.0f} "
              f"L/S {Decimal(row['count_long_short_ratio']):.2f} "
              f"top {Decimal(row['count_toptrader_long_short_ratio']):.2f}")

    spot = {ts: close for ts, close in read_klines(spot_path)}
    perp = {ts: close for ts, close in read_klines(perp_path)}
    print("spot/perp midday closes 22/23/24:")
    for day, hour in [(22, 12), (23, 12), (24, 12)]:
        ts = 1679443200000 + (day - 22) * 86400000 + hour * 3600000
        print(f"  2023-03-{day} {hour:02d}:00 spot {spot.get(ts, 'MISSING')} "
              f"perp {perp.get(ts, 'MISSING')}")


if __name__ == "__main__":
    main()
