#!/usr/bin/env python3
"""Research note 009 analysis: session regimes in print flow, spot vs perp.

Notes 001-003 deferred three questions: burst statistics split by session,
same-millisecond batching by venue, and what simultaneity means for fills.
This answers the first two over one full week (2024-01-15..21, Mon-Sun) of
Binance BTCUSDT aggTrades on spot and USDT-perp: ~15M prints, streamed
(never held), bucketed by UTC session (Asia 00-08, EU 08-16, US 16-24).

Usage:
    python3 research/scripts/sessions_009.py --data-dir /tmp/sess009
Downloads 14 pinned archive slices (verifies CSV content SHA-256 while
streaming — nothing held in memory), analyzes, prints the tables pasted
into research/009-sessions.md. Stdlib only.
"""

import argparse
import hashlib
import io
import pathlib
import urllib.request
import zipfile
from decimal import Decimal

DAYS = ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18",
        "2024-01-19", "2024-01-20", "2024-01-21"]
# Pinned SHA-256 of extracted CSV bytes (container variance documented in
# note 008: pin the input, not the envelope). Verified by from-scratch rerun.
SPOT_CSV = [
    "09dc409893ab672aa00c8aa816490e6ff82891e9925bac3bc7f44b8f87b1e633",
    "4de75433529c42bff47dee2a64264d5321ca7813d441cc92a077d70a5f7749f8",
    "77b2e3084000a32b2f7ffb77cf5c904c6df998d212b5acb226fbb679ae1e5d50",
    "ef886b327972d1a9dac5b828bbc840c358e4432e97655ea796c75d67108bff9a",
    "1b074630588892b97020da95be955dfeef7a1199aae0339a4a5ea100f3727c85",
    "7c9c838c28bbc6bf95069856ca64e982c87bacfd7d4fe243e52064a05a6cb7fa",
    "d2b2b66711082be8b7727c7864c5085509e7b8828ead02fbbc4a58464107fa08",
]
PERP_CSV = [
    "66d19dadb9542d0145d9dafefb19b5195c5ff81838e0250a525d3508aa20d736",
    "609430ee0cce52625d2e51a32627c2ee9ad3452196b33eebe542b82926fd70d2",
    "537c4572c012ddee762f2acda416a35b7548aa772e387b006cc36c136ab13379",
    "6a437c28860b130bb6f9d5ba1537c9e1e09a233653f34314fa7a55a41b2337e7",
    "ab6ea608112b4f4b2362cb2ab1601118199f1ac73cbbf4dc0f058f287f35248b",
    "95b37d91d3594a632e0d0242eeb0a4382b62d1682982b26e287959a17081838b",
    "e8e39e388f427dbc28fc39238cd82c45e3a0cd28666bf34b72fa9cd305a51e66",
]
SPOT_URL = ("https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/"
            "BTCUSDT-aggTrades-{day}.zip")
PERP_URL = ("https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/"
            "BTCUSDT-aggTrades-{day}.zip")

SESSIONS = [("asia", 0, 8), ("eu", 8, 16), ("us", 16, 24)]


def session_of(hour: int) -> str:
    for name, start, end in SESSIONS:
        if start <= hour < end:
            return name
    raise AssertionError(hour)


def stream_csv(path: pathlib.Path):
    with zipfile.ZipFile(path) as archive:
        with archive.open(archive.namelist()[0]) as handle:
            text = io.TextIOWrapper(handle, encoding="utf-8")
            for line in text:
                yield line


def analyze(path: pathlib.Path, want: str, label: str) -> dict:
    digest = hashlib.sha256()
    prints = 0
    volume = Decimal("0")
    taker_buy = 0
    per_second: dict = {}
    per_hour_prints = [0] * 24
    per_hour_vol = [Decimal("0")] * 24
    per_hour_buy = [0] * 24
    same_ms_runs = 0
    same_ms_prints = 0
    prev_ts = -1
    run_open = False
    with zipfile.ZipFile(path) as archive:
        with archive.open(archive.namelist()[0]) as handle:
            text = io.TextIOWrapper(handle, encoding="utf-8")
            for raw in text:
                digest.update(raw.encode("utf-8"))
                parts = raw.strip().split(",")
                if len(parts) < 7:
                    continue
                try:
                    ts = int(parts[5])
                except ValueError:
                    continue  # header row (perp files carry one)
                price = Decimal(parts[1])
                qty = Decimal(parts[2])
                # Spot files write True/False, perp files true/false.
                buyer_maker = parts[6].lower() == "true"
                prints += 1
                volume += price * qty
                taker_buy += 0 if buyer_maker else 1
                second = ts // 1000
                per_second[second] = per_second.get(second, 0) + 1
                hour = (ts // 3600000) % 24
                per_hour_prints[hour] += 1
                per_hour_vol[hour] += price * qty
                per_hour_buy[hour] += 0 if buyer_maker else 1
                if ts == prev_ts:
                    if not run_open:
                        run_open = True
                        same_ms_runs += 1
                    same_ms_prints += 1
                else:
                    run_open = False
                prev_ts = ts
    assert digest.hexdigest() == want, f"{label}: content hash mismatch"
    seconds = sorted(per_second)
    counts = sorted(per_second.values())
    silent = sum(1 for a, b in zip(seconds, seconds[1:]) if b - a > 1)
    return {
        "prints": prints,
        "buy": taker_buy,
        "volume": volume,
        "taker_buy_share": taker_buy / prints,
        "p50_s": counts[len(counts) // 2],
        "p99_s": counts[int(len(counts) * 0.99)],
        "max_s": counts[-1],
        "silent_gaps": silent,
        "same_ms_runs": same_ms_runs,
        "same_ms_share": same_ms_prints / prints,
        "per_hour_prints": per_hour_prints,
        "per_hour_vol": per_hour_vol,
        "per_hour_buy": per_hour_buy,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    root = pathlib.Path(args.data_dir)
    root.mkdir(parents=True, exist_ok=True)

    venues = {"spot": (SPOT_URL, SPOT_CSV, "s"), "perp": (PERP_URL, PERP_CSV, "a")}
    for venue, (template, hashes, prefix) in venues.items():
        total_prints = 0
        total_volume = Decimal("0")
        total_buy = 0
        hour_prints = [0] * 24
        hour_vol = [Decimal("0")] * 24
        hour_buy = [0] * 24
        day_prints = []
        day_max_s = 0
        day_p50_s = []
        total_runs = 0
        total_same_ms = 0
        for day, want in zip(DAYS, hashes):
            local = root / f"{prefix}-{day}.zip"
            if not local.exists():
                urllib.request.urlretrieve(template.format(day=day), local)
            result = analyze(local, want, f"{venue} {day}")
            print(f"{venue} {day}: prints={result['prints']} "
                  f"same_ms={result['same_ms_share']:.1%}")
            total_prints += result["prints"]
            total_volume += result["volume"]
            total_buy += result["buy"]
            day_prints.append(result["prints"])
            for hour in range(24):
                hour_prints[hour] += result["per_hour_prints"][hour]
                hour_vol[hour] += result["per_hour_vol"][hour]
                hour_buy[hour] += result["per_hour_buy"][hour]
            day_max_s = max(day_max_s, result["max_s"])
            day_p50_s.append(result["p50_s"])
            total_runs += result["same_ms_runs"]
            total_same_ms += round(result["same_ms_share"] * result["prints"])

        weekend = sum(day_prints[5:7]) / total_prints
        print(f"== {venue} week: prints={total_prints} volume={total_volume:.0f} "
              f"taker_buy={total_buy / total_prints:.1%} "
              f"same_ms={total_same_ms / total_prints:.1%} ({total_runs} runs) "
              f"p50_s~{sorted(day_p50_s)[len(day_p50_s) // 2]} max_s={day_max_s} "
              f"weekend={weekend:.1%}")
        for name, start, end in SESSIONS:
            prints = sum(hour_prints[start:end])
            volume = sum(hour_vol[start:end], Decimal("0"))
            buy = sum(hour_buy[start:end])
            print(f"  {name}: prints={prints / total_prints:.1%} "
                  f"volume={volume / total_volume:.1%} "
                  f"taker_buy={buy / prints:.1%}")


if __name__ == "__main__":
    main()
