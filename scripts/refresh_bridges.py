"""Fetch all bridge token registries (Squid, Axelar, Wormhole, Synapse, Hop, DefiLlama)."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from core import refresh_all_bridges
from utils import close_redis, setup_logging


async def main() -> None:
    setup_logging()
    results = await refresh_all_bridges()
    print("\n=== Bridge refresh summary ===")
    for source, stats in results.items():
        print(f"  {source:<12} {stats}")
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
