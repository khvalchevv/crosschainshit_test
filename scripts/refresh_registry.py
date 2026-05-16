"""
One-off / manual registry rebuild: CoinGecko coins/list + the bridged
(Wormhole + LayerZero) JSON. main.py also does this automatically on startup
and every 24h — run this only to force an immediate rebuild.

Does a SAFE purge first (registry keys only) — never touches the blacklist
(cc2_blacklist*), subscribers (cc2_subscribers) or cooldowns. Use this
instead of `redis-cli FLUSHDB`, which would wipe user data.

    python scripts/refresh_registry.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import TokenMapper
from utils import close_redis, get_redis, setup_logging


async def main() -> None:
    setup_logging()
    await get_redis()
    mapper = TokenMapper()
    await mapper.purge_registry()        # safe: registry keys only
    stats = await mapper.refresh(force=True)
    print("Registry refresh stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
