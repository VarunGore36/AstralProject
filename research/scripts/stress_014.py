#!/usr/bin/env python3
"""Research note 014 analysis: imbalance memory in the Luna extinction week.

Note 013 measured one crisis (SVB, r=+0.49). A single crisis is an anecdote
with error bars — this reruns the identical method over 2022-05-08..14
(Luna/UST death spiral, BTC ~$35k->$26k): hourly taker-buy shares, lag-1
overall, same-session pairs, high/low-magnitude splits. Pearson with
1/sqrt(n) beside every r.

Usage:
    python3 research/scripts/stress_014.py --data-dir /tmp/stress014
Downloads seven pinned archive slices (verifies CSV content SHA-256 while
streaming — ~200MB never held), analyzes, prints the tables pasted into
research/014-luna-memory.md. Stdlib only.
"""

import argparse
import hashlib
import io
import pathlib
import statistics
import urllib.request
import zipfile

DAYS = ["2022-05-08", "2022-05-09", "2022-05-10", "2022-05-11",
        "2022-05-12", "2022-05-13", "2022-05-14"]
SPOT_URL = ("https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/"
            "BTCUSDT-aggTrades-{day}.zip")
# Pinned SHA-256 of extracted CSV bytes. Computed from verified downloads;
# every later run re-verifies (same discipline as notes 009-013).
SPOT_CSV = [
    "9d2e000e3b1e5ecc6684de250d8584caaa686ed40a5e39cf7742935fdfaf4b3b",
    "bb72b84d098309fa1910529d78ee1652b9d8afe301aa1bfda8f32473647d2005",
    "ac6396f638c35ebc90b69024d857f514ad6b987cabff0875aaebd03694ff5f6d",
    "bb23c14d203c9faacb8733ef86ab6b48362b8c6457e79aa287f3e0fa476ab832",
    "7f99a939c5667f0b7b57c89c56291ba3bb53b16b401a16e35ab7b3168efce22f",
    "ec0e607f1b1324444eba8177568bc047c0c23a4a2847e29662228a8bd96090ee",
    "46592fc58b7739f1adfd73fd19131bd3e6a7cf26ac577d24ccf9be301da054d9",
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
