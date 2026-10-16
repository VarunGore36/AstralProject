#!/usr/bin/env python3
"""Research note 016 analysis: the inverse carry in its best month.

Notes 004-008 ran long-spot/short-perp everywhere. The matrix has an empty
cell: short-spot/long-perp, which collects funding when rates go negative.
November 2022 is its best case (32/90 payments negative, the most hostile
month in the series). Unconditional hold of the full month, $10k legs,
5 bps taker, fills at first/last closes — plus borrow sensitivity, because
short spot pays borrow and the rate is an assumption, not data: 0/2/5/10%
APR over the 30-day hold, every rate reported, none chosen.

Usage:
    python3 research/scripts/inverse_016.py --data-dir /tmp/inv016
Downloads three pinned archive slices (verifies CSV content SHA-256),
analyzes, prints the tables pasted into research/016-inverse-carry.md.
Stdlib only. Funding pins identical to note 005's for the same files.
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
DAYS_HELD = Decimal("30")
BORROW_APRS = [Decimal("0"), Decimal("0.02"), Decimal("0.05"), Decimal("0.10")]

FILES = {
    # url suffix, local name, pinned CSV content SHA (note-005 pins, re-verified)
    "funding": (
        "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2022-11.zip",
        "98b0632629a758e333254e183ea9a6349ad57335b06d7b0ff67c2732cbb75253",
    ),
    "spot": (
        "data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2022-11.zip",
        "d13d85cf3511c3acc7d30114141b3f839db21eb731c0d57782de9480328a8da1",
    ),
    "perp": (
        "data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2022-11.zip",
        "2a3e04a0d7d8e320e86af9c411e8b18936c813672136314a4fca259b4355d352",
    ),
}
BASE = "https://data.binance.vision/"


def fetch(data_dir: pathlib.Path) -> dict:
    data_dir.mkdir(parents=True, exist_ok=True)
    out = {}
    for kind, (suffix, want) in FILES.items():
        local = data_dir / suffix.split("/")[-1].replace(".zip", f".{kind}.zip")
        if not local.exists():
            urllib.request.urlretrieve(BASE + suffix, local)
        with zipfile.ZipFile(local) as archive:
            raw = archive.read(archive.namelist()[0])
        digest = hashlib.sha256(raw).hexdigest()
        assert digest == want, f"{kind}: {digest} != pinned {want}"
        out[kind] = raw
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    raw = fetch(pathlib.Path(args.data_dir))

    funding = [(int(r["calc_time"]), Decimal(r["last_funding_rate"]))
               for r in csv.DictReader(io.StringIO(raw["funding"].decode()))]
    spot = [(int(r[0]), Decimal(r[4])) for r in csv.reader(io.StringIO(raw["spot"].decode()))
            if r[0] != "open_time"]
    perp = [(int(r[0]), Decimal(r[4])) for r in csv.reader(io.StringIO(raw["perp"].decode()))
            if r[0] != "open_time"]
    print(f"payments={len(funding)} spot_bars={len(spot)} perp_bars={len(perp)}")
    neg = sum(1 for _, rate in funding if rate < 0)
    print(f"negative_payments={neg}/{len(funding)}")

    # Inverse collects what the long-short side pays: sign-flipped funding.
    received = sum(-rate * NOTIONAL for _, rate in funding)
    fees = 4 * FEE_PER_FILL
    # Basis drift for short-spot/long-perp: gains when the premium widens.
    drift = (perp[-1][1] - spot[-1][1]) - (perp[0][1] - spot[0][1])
    size = NOTIONAL / spot[0][1]
    drift_pnl = drift * size
    print(f"funding_received={received:.2f} fees={fees:.2f} drift={drift_pnl:.2f}")
    print(f"spot {spot[0][1]} -> {spot[-1][1]} | basis {perp[0][1] - spot[0][1]:.2f} -> "
          f"{perp[-1][1] - spot[-1][1]:.2f}")
    for apr in BORROW_APRS:
        borrow = NOTIONAL * apr * DAYS_HELD / Decimal("365")
        net = received - fees + drift_pnl - borrow
        print(f"borrow_apr={float(apr) * 100:.0f}% borrow_cost={borrow:.2f} net={net:.2f}")


if __name__ == "__main__":
    main()
