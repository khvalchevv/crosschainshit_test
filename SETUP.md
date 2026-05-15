# Cross-chain Arbitrage Scanner — Setup

Real-time cross-chain arbitrage scanner across 55 EVM + non-EVM chains. On-chain price reading (Uniswap V2/V3/V4), DexScreener / DefiLlama / GeckoTerminal fallbacks, Telegram alerting.

## Prerequisites

### 1. Python 3.11+
```bash
python --version   # must be >= 3.11
```

### 2. Redis
- **Windows:** install [Memurai](https://www.memurai.com/get-memurai) (free Redis-compatible server). Runs on default port 6379.
- **macOS:** `brew install redis && brew services start redis`
- **Linux:** `sudo apt install redis-server && sudo systemctl start redis`

Sanity check:
```bash
redis-cli ping     # → PONG
```

### 3. Alchemy API keys
Required for on-chain price reading (Uniswap V2/V3/V4 via Multicall3).
- Sign up at https://dashboard.alchemy.com (PAYG plan — free tier is rate-limited)
- Create 1-3 apps (any single app covers all supported chains)
- Multiple keys = round-robin parallelism across RPC calls — noticeably faster

### 4. Telegram bot
- Create bot via [@BotFather](https://t.me/BotFather) → get token
- `TELEGRAM_CHAT_ID` can be left blank — the bot broadcasts to every user that runs `/start`

### 5. Proxies (optional but strongly recommended)
For DexScreener + CoinMarketCap throughput. Without proxies you'll hit rate limits fast.
- Any HTTP proxy list in `http://user:pass@host:port` format, one per line
- Tested with [Webshare](https://www.webshare.io) residential plan
- Place file at `data/proxies.txt`

## Install

```bash
git clone <repo_url> crosschain_arb
cd crosschain_arb

python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac: source .venv/bin/activate

pip install -r requirements.txt

cp .env.example .env
# edit .env: fill TELEGRAM_BOT_TOKEN, ALCHEMY_KEYS
```

## First run

One-off: populate the token mapper (CoinGecko + LayerZero + bridge registries).
```bash
python scripts/refresh_tokens.py       # CoinGecko + LayerZero
python scripts/refresh_cmc.py          # CoinMarketCap (optional, ~5 min, adds ~1500 tokens)
python scripts/refresh_bridges.py      # LiFi, Axelar, Wormhole, Across, Symbiosis
```

Then start the bot:
```bash
python main.py
```

On first start the bot:
1. Loads token mappings from Redis
2. Warms up on-chain pool discovery (~10-15 min — discovers V2/V3/V4 pools for each token across chains)
3. Indexes Uniswap V4 Initialize events on 13 chains (~20-30 min, one-time, resumable)
4. Starts monitor + detector loops

Alerts appear in Telegram to every user who has sent `/start` to the bot.

## Bot commands (Telegram)

- `/start` — subscribe to alerts
- `/check <cg_id|contract_addr>` — show current prices for a token across all chains the bot knows it on
- `/blacklist add <cg_id>` — suppress alerts for a token
- `/blacklist remove <cg_id>` — unblock
- `/blacklist list` — show blacklist

## Tuning

Edit `config/thresholds.yaml`:
- `min_profit_percent` (default 6.0) — minimum net spread (after bridge costs) to alert
- `alert_min_pool_liquidity_usd` (default 1000) — skip pools below this
- `alert_min_pool_volume_usd` (default 1000) — skip stale pools
- `monitor.interval_sec` (default 5) — monitor loop pacing; cycle itself takes ~20-25s for 12k addresses
- `alert_cooldown_sec` (default 86400) — cooldown per token+chain-pair

## Notes

- Data lives in Redis DB 1 by default. Use a dedicated DB if running other projects on the same Redis.
- Token mapping is cached in Redis for 7 days. Re-run `refresh_tokens.py` weekly to pick up new listings.
- Uniswap V4 pool indexer is resumable — it tracks per-chain cursors in Redis key `cc2:v4_indexer_progress`.
