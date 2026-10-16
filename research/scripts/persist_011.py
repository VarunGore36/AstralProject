#!/usr/bin/env python3
"""Research note 011 analysis: does an imbalanced hour predict the next one?

Note 009 deferred this: spot taker-buy share slides by session, but is it
persistent or noise? Hourly taker-buy shares over the same week
(2024-01-15..21 spot aggTrades), lag-1 autocorrelation overall, per session
(same-session consecutive hours only — no cross-boundary pairs), and for
imbalance magnitude (|share - 0.5|). Pearson via statistics.correlation;
standard error 1/sqrt(n) reported beside every r, labeled plainly.

Usage:
    python3 research/scripts/persist_011.py --data-dir /tmp/persist011
Downloads seven pinned archive slices (verifies CSV content SHA-256 while
streaming), analyzes, prints the tables pasted into
research/011-persistence.md. Stdlib only.
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
# Pinned SHA-256 of extracted CSV bytes; identical to the spot pins behind
# note 009 for the same files (re-verified on every download here).
SPOT_CSV = [
    "09dc409893ab672aa00c8aa816490e6ff82891e9925bac3bc7f44b8f87b1e633",
    "4de75433529c42bff47dee2a64264d5321ca7813d441cc92a077d70a5f7749f8",
    "77b2e3084000a32b2f7ffb77cf5c904c6df998d212b5acb226fbb679ae1e5d50",
    "ef886b327972d1a9dac5b828bbc840c358e4432e97655ea796c75d67108bff9a",
    "1b074630588892b97020da95be955dfeef7a1199aae0339a4a5ea100f3727c85",
    "7c9c838c28bbc6bf95069856ca64e982c87bacfd7d4fe243e52064a05a6cb7fa",
    "d2b2b66711082be8b7727c7864c5085509e7b8828ead02fbbc4a58464107fa08",
]
SPOT_URL = ("https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/"
            "BTCUSDT-aggTrades-{day}.zip")

SESSIONS = [("asia", 0, 8), ("eu", 8, 16), ("us", 16, 24)]


def session_of(hour: int) -> str:
    for name, start, end in SESSIONS:
        if start <= hour < end:
            return name
    raise AssertionError(hour)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    root = pathlib.Path(args.data_dir)
    root.mkdir(parents=True, exist_ok=True)

    # Per-hour (buy prints, total prints), keyed by epoch hour, streamed.
    hours: dict = {}
    for day, want in zip(DAYS, SPOT_CSV):
        local = root / f"s-{day}.zip"
        if not local.exists():
            urllib.request.urlretrieve(SPOT_URL.format(day=day), local)
        digest = hashlib.sha256()
        buy = tot = 0
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
        print(f"{day}: {sum(t for _, t in per_hour.values())} prints")
        hours.update(per_hour)

    ordered = sorted(hours)
    shares = {key: hours[key][0] / hours[key][1] for key in ordered}
    print(f"hours={len(ordered)} overall_buy={sum(h[0] for h in hours.values()) / sum(h[1] for h in hours.values()):.1%}")

    def report(label: str, pairs: list) -> None:
        xs = [shares[a] for a, _ in pairs]
        ys = [shares[b] for _, b in pairs]
        r = statistics.correlation(xs, ys) if len(pairs) > 2 else float("nan")
        by_key = {key: abs(shares[key] - 0.5) for key in ordered}
        xm = [by_key[a] for a, _ in pairs]
        ym = [by_key[b] for _, b in pairs]
        rm = statistics.correlation(xm, ym) if len(pairs) > 2 else float("nan")
        se = 1 / (len(pairs) ** 0.5)
        print(f"{label}: n={len(pairs)} r_share={r:+.2f} r_magnitude={rm:+.2f} se~{se:.2f}")

    report("all consecutive hours", list(zip(ordered, ordered[1:])))
    for name, start, end in SESSIONS:
        keys = [k for k in ordered if start <= ((k * 3600000) // 3600000 % 24) < end]
        # Consecutive hours both inside the session (epoch math, no wall clock).
        pairs = [(a, b) for a, b in zip(keys, keys[1:]) if b - a == 1]
        report(f"session {name}", pairs)


if __name__ == "__main__":
    main()
