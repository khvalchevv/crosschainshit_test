# Cross-Chain Spread Scanner — Setup

Real-time scanner that detects the same token trading at different prices
across chains and alerts via Telegram. It does **not** execute or check
bridges — it only surfaces the spread (bridge feasibility is a separate
concern).

No paid API keys. Price data is free & keyless (DexScreener +
GeckoTerminal); throughput comes from a proxy pool, not API plans.

## How it works

```
REGISTRY   CoinGecko coins/list  +  data/all_bridged_tokens.json
           (Wormhole + LayerZero)        →  cg2:group:{id} = {chain: addr}

MONITOR    every interval_sec: all addresses → DexScreener (price + liq),
           GeckoTerminal as price-only fallback  →  cc2:price:* (TTL)

DETECTOR   every detector_interval_sec: per group spread = (max-min)/min;
           ≥ min_profit_percent  AND  both pools ≥ alert liquidity  AND
           not on cooldown  AND  not blacklisted  →  Telegram alert
```

## Prerequisites

1. **Python 3.12** (3.13 lacks prebuilt wheels for the pinned deps).
2. **Redis** on `localhost:6379` (Memurai on Windows, or a portable
   `redis-server.exe`). DB index 1 by default.
3. **Telegram bot token** from [@BotFather](https://t.me/BotFather).
4. **Proxies** (recommended) — `data/proxies.txt`, one per line, either
   `http://user:pass@host:port` or webshare `host:port:user:pass`.

## Install

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt

cp .env.example .env
# edit .env: set TELEGRAM_BOT_TOKEN
```

## Run

```bash
python main.py
```

On startup the bot builds the registry (CoinGecko + bridged JSON), then
runs the monitor + detector loops. It re-refreshes the registry every 24h.
To force an immediate registry rebuild:

```bash
python scripts/refresh_registry.py
```

Alerts go to every user who has sent `/start` to the bot.

## Telegram commands

- `/start` — subscribe to alerts
- `/status` — scanner stats
- `/blacklist add <id>` — mute a token (`remove`, `list`, `clear`)
- `/stop` — unsubscribe

## Tuning — `config/thresholds.yaml`

- `min_profit_percent` (4.0) — minimum spread to alert
- `alert_min_pool_liquidity_usd` (10000) — both pools must clear this
- `min_pool_liquidity_usd` (500) — below this a pool's price is ignored
- `interval_sec` (10) — price polling cadence
- `detector_interval_sec` (60) — spread scan cadence
- `alert_cooldown_sec` (3600) — per token+chain-pair+spread-bucket

## Notes

- Data lives in Redis DB 1. Registry cached 7 days; auto-refreshed every 24h.
- `data/all_bridged_tokens.json` is the Wormhole+LayerZero registry — refresh
  it from your source periodically to pick up new bridged tokens.
