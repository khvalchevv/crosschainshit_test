# Deploy to a server (Linux)

## 1. What to copy

Copy the project dir, but NOT these (rebuilt on the server):
- `.venv/`            — recreate on server
- `redis-portable/`   — server uses its own Redis
- `__pycache__/`, `*.pyc`

Must include:
- all code (`core/`, `utils/`, `config/`, `scripts/`, `main.py`)
- `requirements-lock.txt`   ← exact working deps (use THIS, not requirements.txt)
- `.env`
- `data/proxies.txt`
- `data/all_bridged_tokens.json`
- `data/dead_tokens.json`

```bash
# from this machine (example)
rsync -av --exclude .venv --exclude redis-portable --exclude __pycache__ \
  crosschain/ user@server:/opt/crosschain/
```

## 2. Server prerequisites

```bash
sudo apt update
sudo apt install -y python3.12 python3.12-venv redis-server
sudo systemctl enable --now redis-server
redis-cli ping        # -> PONG
```

## 3. Install

```bash
cd /opt/crosschain
python3.12 -m venv .venv
.venv/bin/pip install -U pip
.venv/bin/pip install -r requirements-lock.txt   # NOT requirements.txt
```

Check `.env`:
```
REDIS_URL=redis://localhost:6379/1
TELEGRAM_BOT_TOKEN=<same token>
PROXIES_FILE=data/proxies.txt
```

## 4. First run / registry

The bot builds the registry on startup (CoinGecko + bridged JSON, ~1–2 min).
To pre-build (safe, never touches blacklist/subscribers):

```bash
.venv/bin/python scripts/refresh_registry.py
```

## 5. Carry over the curated blacklist (optional but recommended)

Blacklist + subscribers live in Redis; a fresh server Redis is empty.

Export here:
```bash
redis-portable/redis-cli.exe -n 1 SMEMBERS cc2_blacklist     > bl.txt
redis-portable/redis-cli.exe -n 1 SMEMBERS cc2_blacklist_leg  > bl_leg.txt
```
Import on server:
```bash
[ -s bl.txt ]     && redis-cli -n 1 SADD cc2_blacklist     $(cat bl.txt)
[ -s bl_leg.txt ] && redis-cli -n 1 SADD cc2_blacklist_leg  $(cat bl_leg.txt)
```

Subscribers: just `/start` the bot again in Telegram (same bot token → same
bot; the subscriber set just needs re-populating on the new Redis).

Clean start instead: after the bot has run ~2 cycles, run
`.venv/bin/python scripts/snapshot_blacklist.py` to mute everything that is
already spreading, so you only watch NEW spreads form.

## 6. Run it (systemd — survives reboot/disconnect)

`/etc/systemd/system/crosschain.service`:
```ini
[Unit]
Description=crosschain spread scanner
After=network-online.target redis-server.service
Wants=network-online.target

[Service]
WorkingDirectory=/opt/crosschain
ExecStart=/opt/crosschain/.venv/bin/python main.py
Restart=always
RestartSec=5
User=YOURUSER

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crosschain
journalctl -u crosschain -f          # live logs
```

Quick alternative without systemd:
```bash
nohup .venv/bin/python main.py > bot.log 2>&1 &
tail -f bot.log
```

## 7. Watching latency

Key log lines:
- `cc_monitor.cycle_done … elapsed=` — full price sweep time
- `phase_timing ds_sec= jup_sec= gt_sec=` — per-source timing
- `cc_detector.cycle_done … elapsed=` — spread scan time (event-driven, fires
  right after each monitor sweep)
- `cc_detector.opportunity` → `alerter.broadcast_complete` — detection→sent

End-to-end (price move → Telegram) ≈ monitor cadence (~5 s) + detector
(~0.5 s) + send (~1–2 s) ≈ **~7–10 s**. Server location affects proxy/API
and Telegram round-trips — test from the region you'll run in.

## Notes

- Same Telegram bot token = same bot. Don't run two instances on the same
  token simultaneously (Telegram long-polling conflict). Stop the local one
  before starting the server one.
- Registry auto-refreshes every 24 h (safe purge, keeps blacklist).
- `requirements.txt` has the original (broken on 3.12/3.13) pins — always
  install from `requirements-lock.txt`.
