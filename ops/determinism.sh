#!/usr/bin/env bash
# Determinism gate over a pinned fixture capture (8 real Binance depth
# frames, 3 chunks, fully deterministic manifest).
#
#   ops/determinism.sh check   normalize twice (byte equality), replay once
#                              (pinned signal hash), hash the parquet tree
#                              (pinned hash). Used by CI on every push.
#   ops/determinism.sh pin     regenerate ops/determinism/expected after an
#                              intentional engine change. Review the diff like
#                              a golden test — a changed pin means behavior
#                              changed, which is either the point or a bug.
#
# The tree pin is toolchain-sensitive by design: a parquet/arrow upgrade
# may repackage identical rows into different bytes (writer version stamps),
# which correctly fails `check` until a human re-pins. The signal pin is
# toolchain-insensitive (pure Rust hashing of strategy bytes) and must only
# ever change with the strategies or the fixture.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURE="$ROOT/ops/determinism/capture"
EXPECTED="$ROOT/ops/determinism/expected"
MODE="${1:-check}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cargo build -q -p astra-normalize -p astra-replay

./target/debug/astra-normalize --input "$FIXTURE" --output "$work/out1" >/dev/null
./target/debug/astra-normalize --input "$FIXTURE" --output "$work/out2" >/dev/null
diff -r "$work/out1" "$work/out2"
echo "normalize-twice: byte-identical"

signal="$(./target/debug/astra-replay --input "$FIXTURE" --strategy book-top --seed 7 | awk '/^signal_hash/ {print $2}')"
tree="$(cd "$work/out1" && find . -name '*.parquet' | sort | xargs sha256sum | sha256sum | cut -d' ' -f1)"

if [ "$MODE" = "pin" ]; then
    printf 'signal_hash=%s\ntree_hash=%s\n' "$signal" "$tree" > "$EXPECTED"
    echo "pinned $EXPECTED"
    exit 0
fi

# shellcheck disable=SC1090
source "$EXPECTED"
if [ "$signal" != "$signal_hash" ]; then
    echo "MISMATCH: replay signal_hash $signal != pinned $signal_hash" >&2
    exit 1
fi
if [ "$tree" != "$tree_hash" ]; then
    echo "MISMATCH: parquet tree $tree != pinned $tree_hash" >&2
    exit 1
fi
echo "pins match: signal_hash=$signal tree_hash=$tree"
