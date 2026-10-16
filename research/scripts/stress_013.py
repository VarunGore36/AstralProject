#!/usr/bin/env python3
"""Research note 013 analysis: does imbalance memory spike under stress?

Notes 011/012 measured lag-1 taker-buy persistence in a calm week
(+0.36 spot, concentrated in loud hours, weaker on perp). This reruns the
identical method over 2023-03-06..12 — SVB collapse (Mar 10), USDC depeg
weekend, ~8x January's print flow — asking whether memory strengthens when
it matters. Hourly buckets, overall plus same-session pairs plus
high/low-magnitude splits. Pearson with 1/sqrt(n) beside every r.

Usage:
    python3 research/scripts/stress_013.py --data-dir /tmp/stress013
Downloads seven pinned archive slices (verifies CSV content SHA-256 while
streaming — ~720MB never held), analyzes, prints the tables pasted into
research/013-stress-memory.md. Stdlib only.
"""

import argparse
import hashlib
import io
import pathlib
import statistics
import urllib.request
import zipfile

DAYS = ["2023-03-06", "2023-03-07", "2023-03-08", "2023-03-09",
        "2023-03-10", "2023-03-11", "2023-03-12"]
SPOT_URL = ("https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/"
            "BTCUSDT-aggTrades-{day}.zip")
# Pinned SHA-256 of extracted CSV bytes. Filled by the first verified run
# below; every later run re-verifies (same discipline as notes 009-012).
SPOT_CSV = [
    "e074202b6548d4ba1a0961a946eb14c81924d58d6842f720d8e229eb62062ee3",
    "97d9035d8eacbe0faa5e973f210b989d9ef20fac223c22242b5fe2e5db48ceb7",
    "7d6107a7399bec5e86063a9b5ad4bb2c777358745a806a57ee68e87e43e93868",
    "b020ddd7ff3aa423fc48656e7ccb339f7d68bfb691a3b1a78556f7e05debff4c",
    "b85ba4602565022933f46da43f241adc8c194a7cf1b2b8b464c41031ab8b3d15",
    "e8ddd7c61ff512197875ffcde932ceb5b65f0e6debd7cc8b44c8a203a934d1a9",
    "7e33231bf7f854308e9c4e4e7ba92b41f75af021c389338b8d3d815413e3ee5c",
]

SESSIONS = [("asia", 0, 8), ("eu", 8, 16), ("us", 16, 24)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    root = pathlib.Path(args.data_dir)
    root.mkdir(parents=True, exist_ok=True)

    hours: dict = {}
    for day, want in zip(DAYS, SPOT_CSV):
        local = root / f"s-{day}.zip"
        if not local.exists():
            urllib.request.urlretrieve(SPOT_URL.format(day=day), local)
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
        total = sum(t for _, t in per_hour.values())
        print(f"{day}: {total} prints")
        hours.update(per_hour)

    ordered = sorted(hours)
    shares = {k: hours[k][0] / hours[k][1] for k in ordered}
    total = sum(t for _, t in hours.values())
    buy = sum(b for b, _ in hours.values())
    print(f"hours={len(ordered)} prints={total} overall_buy={buy / total:.1%}")

    def report(label: str, pairs: list) -> None:
        xs = [shares[a] for a, _ in pairs]
        ys = [shares[b] for _, b in pairs]
        r = statistics.correlation(xs, ys) if len(pairs) > 2 else float("nan")
        print(f"{label}: n={len(pairs)} r={r:+.2f} se~{1 / (len(pairs) ** 0.5):.2f}")

    report("all consecutive hours", list(zip(ordered, ordered[1:])))
    for name, start, end in SESSIONS:
        keys = [k for k in ordered if start <= (k % 24) < end]
        report(f"session {name}", [(a, b) for a, b in zip(keys, keys[1:]) if b - a == 1])
    mags = sorted(abs(shares[k] - 0.5) for k in ordered)
    cutoff = mags[len(mags) // 2]
    report("high-mag-first", [(a, b) for a, b in zip(ordered, ordered[1:])
                               if abs(shares[a] - 0.5) >= cutoff])
    report("low-mag-first", [(a, b) for a, b in zip(ordered, ordered[1:])
                              if abs(shares[a] - 0.5) < cutoff])


if __name__ == "__main__":
    main()
