#!/usr/bin/env python3
"""Research note 008 analysis: full-year 2023 carry, bear grind to bull ramp.

Twelve consecutive months (no chosen windows): unconditional long-$10k
spot / short-$10k perp hold plus the p90/p10 timed rule from note 007, both
over the same bars. Pairing is by timestamp, not position — March carries
one perp-only bar (the 2023-03-24 spot halt) which is dropped and disclosed,
never interpolated. Money in Decimal (exact); floats only for labeled
analytics.

Usage:
    python3 research/scripts/carry_008.py --data-dir /tmp/carry008
Downloads 36 pinned archive slices (verifies SHA-256 first), analyzes,
prints the result tables pasted into research/008-full-year-carry.md.
Stdlib only. Independent script per note.
"""

import argparse
import csv
import datetime
import hashlib
import io
import math
import pathlib
import urllib.request
import zipfile
from decimal import Decimal

NOTIONAL = Decimal("10000")
FEE_BPS = Decimal("5")
FEE_PER_FILL = NOTIONAL * FEE_BPS / Decimal("10000")
WARMUP_HOURS = 720
ENTRY_PCT = 90
EXIT_PCT = 10

MONTHS = [f"2023-{m:02d}" for m in range(1, 13)]
# Pinned SHA-256 of the extracted CSV bytes (not the zip container: the
# CDN re-packages identical CSVs into byte-different zips — observed on
# the July perp file across three downloads with identical CSV. Pin the
# input, not the envelope). Row counts are calendar facts asserted alongside;
# spot files carry no header while perp/funding do, and March spot is one
# bar short (the 2023-03-24 halt bar exists only on perp).
FUNDING_CSV = [
    ("b83a5af331578615875966fb227bfe8e2583e252837310c9bcfb8efa56ba2895", 93),
    ("c346990315d82db4af0e240d9162d7686e57e8b604dfcb4c4e409e1e6ff2ab47", 84),
    ("d776e94a48c8cee3c3131f3055483fed45e9ed2b1c986de077acae73435bc6b4", 93),
    ("1c2b6f3e55fbcbc62981bc271f63821a69b9b1a6c0cff7cb9baa72cee26ff0c9", 90),
    ("a979797e9509ad333d621017460a5be095cd2f641661990e42578519152a36a9", 93),
    ("1d076e090a53813ef35c0c097858214b413d5addad346193a243cb55afed2b2c", 90),
    ("65c6e99359aab03e6370e0dc28aa5f021ae59a3df765f34c3d164f26ef7ca079", 93),
    ("e92d6c334b66ed6e4227b923e58ede256ab2eeafcad8b5554778b21cabc3b8f1", 93),
    ("f7ad0d9e9039b5a7d165b32f0347b52bca9e1ba26b1b5697fc45ebc9fa49610c", 90),
    ("01d36bd8eccb2610c4d6c6dcddddf101a507b47e2c340f8c8074735800116904", 93),
    ("bde8bb8a6a7fa82dbda8663dd2fceacb518266164f15dacdca3675bd7d527a27", 90),
    ("00a808892d8c0bfcd2dfaa8c182b26da4445cbe4895f18c1893349808fe39f33", 93),
]
SPOT_CSV = [
    ("7b144bee86ff9cc38aaf8cec44174073647e96d1e16154d8800bd68dcfe3bf98", 744),
    ("03f56f6468d0d0222807ec89899d896128c91237e2b38dd656049198b582db80", 672),
    ("9608d74f112518d3e4941cd9673f96f887f752120a7ed6c29240a75795f15974", 743),
    ("cda14cdfef3083a108cacdd5b78ef2eedb6027cb165985765b4ee54afb630b2d", 720),
    ("8e4f4cd8c5c7da1f6efe20bcd438a2b1eadd0f70bee4c467fb6d39150be1d632", 744),
    ("57263a3d9ad70409cec337a4311d31edccf6c632c929e9f2d58ebce2fcc4dc26", 720),
    ("4ef25593cf9c80c8c9905333c8beffeef8e26cad0686db53cb90e5f903b085de", 744),
    ("6ef7812f83a1d1e20fff128734b7d294559d081ff184db8be75604ab5e20dde4", 744),
    ("70debbfe04109a118060cad5afcc4bbf062d0c6f37c745afa635491e53403cf6", 720),
    ("c6d89c6c2c8e813ec062db5ba2876e24e4a6f082e105b43d344a735e27b19093", 744),
    ("871064bca3f7681acc12b7ded60de5bc23df01176172be771542770d11fd16bd", 720),
    ("01701aa39bfb54f3507a939df9850289d5e5b5d764410c10b751c45399beb2e4", 744),
]
PERP_CSV = [
    ("825f9ca3c238c39c965272f800d0c153f2f1568d24fad12611b92fbf15bd1edd", 744),
    ("507e59310abaf6e7abefd7c998d4531fb53f4167d18de69c28ac2ce38f6c8134", 672),
    ("064621617146b81b6fa3531b8a0ac8b38e7ffabd642cbb8b5d45836c5d8198fe", 744),
    ("613f36da70c76e94bf1a29025705485ef2ed001951e748f58f2cfcba81a120c4", 720),
    ("334d3083ce32f6769b37760035b005d17da4e5e7f4a1c6968e5b26ce1477db91", 744),
    ("70c7f9ccbd791fc8ddd0df9c890f5baecc4879213d4e4f357a88d84423d93324", 720),
    ("7aa7ad818c44a15f2786babdb6d7386598f03efa69fc17449cb3a2438e0c09e3", 744),
    ("6868898907f431e030361907f7acf4c565ed9778e04a4c97c1ada1a67db6fd72", 744),
    ("b533bee45ac314ebe67df9c5b4a977d56e9448fbc8fb385b9e2f8372242e681b", 720),
    ("4d3ca0e486b62d344b024498a20c796f95ac2d5ff6b7e0a9a8ab9186ca55a9fc", 744),
    ("233a12c4dbdc51a00e0b55194b928f0b6231cc8b43d9489d495e5b72dda9099a", 720),
    ("d0ae55800192da8aee28b0001da12386d9797b0c42d23817cb0fd375af0eb863", 744),
]
BASE = "https://data.binance.vision/"


def fetch(data_dir: pathlib.Path):
    data_dir.mkdir(parents=True, exist_ok=True)

    def get(url: str, local_name: str, want: str, rows: int) -> bytes:
        local = data_dir / local_name
        if not local.exists():
            urllib.request.urlretrieve(url, local)
        with zipfile.ZipFile(local) as archive:
            raw = archive.read(archive.namelist()[0])
        digest = hashlib.sha256(raw).hexdigest()
        assert digest == want, f"{local_name}: CSV {digest} != pinned {want}"
        content = [line for line in raw.decode().split("\n") if line]
        if local_name.startswith(("perp-", "funding-")):
            content = content[1:]
        assert len(content) == rows, f"{local_name}: {len(content)} rows != {rows}"
        return raw

    funding, spot, perp = [], [], []
    for month, (fh, frows), (sh, srows), (ph, prows) in zip(
        MONTHS, FUNDING_CSV, SPOT_CSV, PERP_CSV
    ):
        funding.append(get(
            BASE + f"data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-{month}.zip",
            f"funding-{month}.zip", fh, frows))
        spot.append(get(
            BASE + f"data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-{month}.zip",
            f"spot-{month}.zip", sh, srows))
        perp.append(get(
            BASE + f"data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-{month}.zip",
            f"perp-{month}.zip", ph, prows))
    return funding, spot, perp


def read_funding(raw: bytes):
    text = raw.decode()
    return [(int(r["calc_time"]), Decimal(r["last_funding_rate"]))
            for r in csv.DictReader(io.StringIO(text))]


def read_klines(raw: bytes):
    text = raw.decode()
    return [(int(r[0]), Decimal(r[4])) for r in csv.reader(io.StringIO(text))
            if r[0] != "open_time"]


def month_of(ts_ms: int) -> str:
    return datetime.datetime.fromtimestamp(
        ts_ms / 1000, tz=datetime.timezone.utc).strftime("%Y-%m")


def percentile(sorted_vals, pct: int) -> Decimal:
    rank = max(1, math.ceil(pct / 100 * len(sorted_vals)))
    return sorted_vals[rank - 1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    args = parser.parse_args()
    fund_paths, spot_paths, perp_paths = fetch(pathlib.Path(args.data_dir))

    funding = sorted(r for raw in fund_paths for r in read_funding(raw))
    spot_by_ts: dict = {}
    for raw in spot_paths:
        for ts, close in read_klines(raw):
            spot_by_ts[ts] = close
    perp_by_ts: dict = {}
    for raw in perp_paths:
        for ts, close in read_klines(raw):
            perp_by_ts[ts] = close
    common = sorted(set(spot_by_ts) & set(perp_by_ts))
    dropped = (len(spot_by_ts) - len(common)) + (len(perp_by_ts) - len(common))
    spot = [(ts, spot_by_ts[ts]) for ts in common]
    perp = [(ts, perp_by_ts[ts]) for ts in common]
    print(f"payments={len(funding)} paired_bars={len(common)} dropped_unpairable={dropped}")
    assert dropped <= 4, f"too many unpairable bars: {dropped}"

    size = NOTIONAL / spot[0][1]
    received = sum(rate * NOTIONAL for _, rate in funding)
    drift = (perp[-1][1] - spot[-1][1]) - (perp[0][1] - spot[0][1])
    base_net = received - 4 * FEE_PER_FILL + (-drift * size)
    print(f"unconditional: funding={received:.2f} drift={-drift * size:.2f} "
          f"fees={4 * FEE_PER_FILL:.2f} net={base_net:.2f}")

    by_month: dict = {}
    neg_months: dict = {}
    for ts, rate in funding:
        month = month_of(ts)
        by_month[month] = by_month.get(month, Decimal("0")) + rate * NOTIONAL
        neg_months[month] = neg_months.get(month, 0) + (1 if rate < 0 else 0)
    for month in MONTHS:
        total = sum(1 for ts, _ in funding if month_of(ts) == month)
        print(f"funding {month}: {by_month.get(month, Decimal('0')):.2f} "
              f"({neg_months.get(month, 0)}/{total} negative)")

    # Timed rule over the same bars (January doubles as warmup).
    history: list = []
    holding = False
    start_ts = 0
    start_basis = Decimal("0")
    spans: list = []
    timed_drift = Decimal("0")
    trades = 0
    open_bars = 0
    for index, ((ts, s), (_, p)) in enumerate(zip(spot, perp)):
        basis = p - s
        if index >= WARMUP_HOURS:
            ordered = sorted(history)
            if not holding and basis >= percentile(ordered, ENTRY_PCT):
                holding = True
                start_ts, start_basis = ts, basis
                trades += 1
            elif holding and basis < percentile(ordered, EXIT_PCT):
                holding = False
                spans.append((start_ts, ts))
                timed_drift += -(basis - start_basis) * size
                trades += 1
        history.append(basis)
        if holding:
            open_bars += 1
    if holding:
        spans.append((start_ts, spot[-1][0] + 3600000))
        timed_drift += -((perp[-1][1] - spot[-1][1]) - start_basis) * size
        trades += 1
    funding_got = Decimal("0")
    for ts, rate in funding:
        if any(open_ts <= ts < close_ts for open_ts, close_ts in spans):
            funding_got += rate * NOTIONAL
    timed_net = funding_got - trades * 2 * FEE_PER_FILL + timed_drift
    print(f"timed p90/p10: round_trips~{trades // 2} fills={trades} "
          f"open_bars={open_bars}/{len(spot) - WARMUP_HOURS} "
          f"funding={funding_got:.2f} drift={timed_drift:.2f} net={timed_net:.2f}")

    print(f"spot {spot[0][1]} -> {spot[-1][1]} | perp {perp[0][1]} -> {perp[-1][1]}")


if __name__ == "__main__":
    main()
