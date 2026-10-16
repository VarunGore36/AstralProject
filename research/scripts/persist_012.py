#!/usr/bin/env python3
"""Research note 012 analysis: persistence on perp, and where it concentrates.

Note 011 found spot imbalance memory identical across sessions. Two
questions remain on the same data: does balanced perp flow forget faster,
and does persistence concentrate in strongly imbalanced hours? Hourly
taker-buy shares for both venue weeks behind note 009, lag-1 overall plus
split by first-hour magnitude (above/below median |share - 0.5|).

Usage:
    python3 research/scripts/persist_012.py --data-dir /tmp/persist012
Downloads fourteen pinned archive slices (verifies CSV content SHA-256
while streaming), analyzes, prints the tables pasted into
research/012-persistence-venue.md. Stdlib only. Pins identical to the
note-009 pins for the same files, re-verified here.
"""

import argparse
import hashlib
import io
import pathlib
import statistics
import urllib.request
import zipfile

DAYS = ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18",
        "2024-01-19", "2024-01-20", "2024-01-21"]
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


def load(root: pathlib.Path, template: str, prefix: str, days: list, hashes: list) -> dict:
    hours: dict = {}
    for day, want in zip(days, hashes):
        local = root / f"{prefix}-{day}.zip"
        if not local.exists():
            urllib.request.urlretrieve(template.format(day=day), local)
        digest = hashlib.sha256()
        per_hour: dict = {}
        with zipfile.ZipFile(local) as archive:
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
                        continue
                    buyer_maker = parts[6].lower() == "true"
                    key = ts // 3600000
                    b, t = per_hour.get(key, (0, 0))
                    per_hour[key] = (b + (0 if buyer_maker else 1), t + 1)
        assert digest.hexdigest() == want, f"{day}: content hash mismatch"
        print(f"{prefix} {day}: {sum(t for _, t in per_hour.values())} prints")
        hours.update(per_hour)
    return hours


def report(label: str, shares: dict, pairs: list) -> None:
    xs = [shares[a] for a, _ in pairs]
    ys = [shares[b] for _, b in pairs]
    r = statistics.correlation(xs, ys) if len(pairs) > 2 else float("nan")
    se = 1 / (len(pairs) ** 0.5)
    print(f"{label}: n={len(pairs)} r={r:+.2f} se~{se:.2f}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    root = pathlib.Path(args.data_dir)
    root.mkdir(parents=True, exist_ok=True)

    for venue, template, prefix, hashes in (
        ("spot", SPOT_URL, "s", SPOT_CSV),
        ("perp", PERP_URL, "p", PERP_CSV),
    ):
        hours = load(root, template, prefix, DAYS, hashes)
        ordered = sorted(hours)
        shares = {k: hours[k][0] / hours[k][1] for k in ordered}
        print(f"== {venue}: {len(ordered)} hours ==")
        report(f"{venue} all", shares, list(zip(ordered, ordered[1:])))
        mags = sorted(abs(shares[k] - 0.5) for k in ordered)
        cutoff = mags[len(mags) // 2]
        high = [(a, b) for a, b in zip(ordered, ordered[1:])
                if abs(shares[a] - 0.5) >= cutoff]
        low = [(a, b) for a, b in zip(ordered, ordered[1:])
               if abs(shares[a] - 0.5) < cutoff]
        report(f"{venue} high-mag-first", shares, high)
        report(f"{venue} low-mag-first", shares, low)


if __name__ == "__main__":
    main()
