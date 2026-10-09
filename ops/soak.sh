#!/bin/sh
set -e
cd "$(dirname "$0")/.."

SOAK="${SOAK_DIR:-data/soak-$(date +%Y-%m-%d)}"
BIN="./target/debug/astra-record"
DURATION=259200
RECONNECTS=10000

streams() {
  echo "binance-spot-btc binance spot BTC/USDT book_diff wss://data-stream.binance.vision/ws/btcusdt@depth@100ms"
  echo "binance-spot-eth binance spot ETH/USDT book_diff wss://data-stream.binance.vision/ws/ethusdt@depth@100ms"
  echo "bybit-spot-btc bybit spot BTC/USDT book_diff"
  echo "bybit-perp-btc bybit perp BTC/USDT book_diff"
}

cmd_start() {
  cargo build -p astra-record
  mkdir -p "$SOAK"
  streams | while read -r name venue market symbol channel url; do
    out="$SOAK/$name"
    mkdir -p "$out"
    if [ -n "$url" ]; then url_flag="--url $url"; else url_flag=""; fi
    # shellcheck disable=SC2086
    nohup $BIN capture --output "$out" \
      --venue "$venue" --market "$market" --symbol "$symbol" --channel "$channel" \
      --duration-secs "$DURATION" --max-reconnects "$RECONNECTS" $url_flag \
      > "$out.log" 2>&1 &
    echo "$name pid $!"
  done
}

cmd_status() {
  streams | while read -r name venue market symbol channel url; do
    frames=$(grep -h "^frames" "$SOAK/$name.log" 2>/dev/null || echo "not finished")
    if pgrep -f "astra-record capture --output $SOAK/$name" > /dev/null; then
      echo "$name RUNNING ($frames)"
    else
      echo "$name STOPPED ($frames)"
    fi
  done
}

cmd_check() {
  tmp="$(mktemp)"
  streams > "$tmp"
  failures=0
  while read -r name venue market symbol channel url; do
    echo "=== $name ==="
    if output="$($BIN check --input "$SOAK/$name" 2>&1)"; then
      echo "$output" | tail -n 14
      case "$output" in
        *"verdict     healthy"*)
          echo "verdict     $name healthy"
          ;;
        *)
          echo "verdict     $name ISSUES (see above)"
          failures=$((failures + 1))
          ;;
      esac
    else
      echo "$output" | tail -n 14
      echo "verdict     $name ERROR (check failed)"
      failures=$((failures + 1))
    fi
  done < "$tmp"
  rm -f "$tmp"
  if [ "$failures" -eq 0 ]; then
    echo "soak_verdict healthy (all streams)"
  else
    echo "soak_verdict issues found: $failures stream(s)"
  fi
  return "$failures"
}

case "${1:-}" in
  start) cmd_start ;;
  status) cmd_status ;;
  check) cmd_check ;;
  *) echo "usage: $0 {start|status|check}" >&2; exit 1 ;;
esac
