# Astral Project

An open, reproducible measurement layer for crypto markets — so you can tell
whether a trading idea actually survives fees, slippage, latency, liquidity and
realistic fills, and reproduce that answer later.

## Status

- **Done** — design and strategy. No code yet.
- **Working on** — `astra-record`: lossless market-data capture for
  {Binance, Bybit} × {BTC/USDT, ETH/USDT} × {spot, USDT-perp},
  raw immutable events → normalized Parquet.
- **Next (one thing)** — rebuild L2 order books from captured data and validate
  them against exchange-published checksums.

## Scope

Historical research and simulation only. No execution, no trading, no capital.
A rigorous "this has no edge" is a successful result here.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
