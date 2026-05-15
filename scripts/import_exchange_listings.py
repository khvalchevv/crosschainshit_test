"""Import multinetwork tokens from a CEX-listings dump file.
Usage: python scripts/import_exchange_listings.py <path-to-file>"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from core.bridge_sources import refresh_exchange_listings
from utils import close_redis, setup_logging


async def main() -> None:
    setup_logging()
    if len(sys.argv) < 2:
        print("Usage: python scripts/import_exchange_listings.py <file>")
        sys.exit(1)
    path = sys.argv[1]
    res = await refresh_exchange_listings(path)
    print(f"\n=== Exchange listings import ===")
    print(f"  contracts:  {res.get('contracts', 0)}")
    print(f"  multichain: {res.get('multichain', 0)}")
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
