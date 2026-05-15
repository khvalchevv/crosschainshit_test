"""Show cross-chain scanner stats: groups, prices, subscribers.

Usage:
    python scripts/stats.py
"""
from __future__ import annotations

import asyncio
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from utils import close_redis, get_redis


async def main() -> None:
    r = await get_redis()

    # Groups
    groups = 0
    multichain = 0
    chain_counter: Counter = Counter()
    distribution: Counter = Counter()

    async for key in r.scan_iter(match="cg2:group:*", count=1000):
        h = await r.hgetall(key)
        if not h:
            continue
        groups += 1
        for c in h.keys():
            chain_counter[c] += 1
        if len(h) >= 2:
            multichain += 1
            distribution[len(h)] += 1

    # Prices
    price_counter: Counter = Counter()
    async for key in r.scan_iter(match="cc2:price:*", count=1000):
        parts = key.split(":")
        if len(parts) >= 3:
            price_counter[parts[2]] += 1

    # Subscribers
    subs = await r.scard("cc2_subscribers")

    # Alerts (last 24h-ish)
    alerts_live = 0
    async for _ in r.scan_iter(match="cc2_alerted:*", count=1000):
        alerts_live += 1

    print("=" * 60)
    print("CROSS-CHAIN SCANNER STATS")
    print("=" * 60)
    print(f"Token groups:        {groups:,}")
    print(f"Multichain tokens:   {multichain:,}")
    print(f"Fresh prices:        {sum(price_counter.values()):,}")
    print(f"Telegram subs:       {subs}")
    print(f"Active dedup keys:   {alerts_live:,}  (alerts in cooldown)")
    print()
    print("Groups per chain:")
    for c, n in sorted(chain_counter.items(), key=lambda x: -x[1]):
        fresh = price_counter.get(c, 0)
        coverage = fresh * 100 // n if n else 0
        print(f"  {c:<12} groups={n:>5,}  prices={fresh:>5,}  ({coverage}%)")
    print()
    print("Chain-count distribution:")
    for n in sorted(distribution):
        print(f"  {n:>2} chains: {distribution[n]:>5,} tokens")

    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
