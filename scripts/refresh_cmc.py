"""Merge CoinMarketCap tokens into Redis (takes ~1-5 min).

Usage:
    python scripts/refresh_cmc.py             # top 5000
    python scripts/refresh_cmc.py --limit 10000
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from core import TokenMapper
from utils import close_redis, setup_logging


async def main() -> None:
    setup_logging()
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=5000, help="Top N tokens by market cap")
    args = p.parse_args()

    mapper = TokenMapper()
    stats = await mapper._refresh_coinmarketcap(limit=args.limit)
    print("\n=== CMC refresh done ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
