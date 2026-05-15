"""Import multichain tokens from wh_wrapped_tokens_normalized.json (Wormhole bulk).
Usage: python scripts/import_wh_normalized.py <path-to-file>"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from core.bridge_sources import refresh_wh_normalized
from utils import close_redis, setup_logging


async def main() -> None:
    setup_logging()
    if len(sys.argv) < 2:
        print("Usage: python scripts/import_wh_normalized.py <file>")
        sys.exit(1)
    path = sys.argv[1]
    res = await refresh_wh_normalized(path)
    print(f"\n=== Wormhole normalized import ===")
    print(f"  contracts:   {res.get('contracts', 0)}")
    print(f"  multichain:  {res.get('multichain', 0)}")
    print(f"  skipped:     {res.get('skipped', 0)}")
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
