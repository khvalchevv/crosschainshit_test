"""Force refresh token mappings from CoinGecko + LayerZero OFT.

Usage:
    python scripts/refresh_tokens.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Allow importing from parent
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import TokenMapper
from utils import close_redis, setup_logging


async def main() -> None:
    setup_logging()
    mapper = TokenMapper()
    stats = await mapper.refresh(force=True)
    print("\nRefresh stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
