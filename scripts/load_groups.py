"""
One-shot: load data/groups_dump.json into Redis under NEW prefixes (cg2:*).
Run this AFTER dump_groups.py and BEFORE first v2 startup so the bot doesn't
have to wait for a token_mapper refresh cycle.

Writes:
  cg2:group:{cg_id}              hash {chain: addr}
  cg2:contract:{chain}:{addr}    string -> cg_id  (reverse lookup)
  cg2:bridges:{cg_id}            set    of bridge tags
  cg2:refreshed_at               string current unix ts
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import get_redis  # type: ignore


IN_PATH = Path(__file__).resolve().parent.parent / "data" / "groups_dump.json"
TTL_SEC = 7 * 24 * 3600  # match what token_mapper would set


async def main() -> None:
    if not IN_PATH.exists():
        print(f"ERROR: {IN_PATH} not found — run scripts/dump_groups.py first")
        sys.exit(1)

    data: dict[str, dict] = json.loads(IN_PATH.read_text(encoding="utf-8"))
    print(f"loaded {len(data)} groups from dump")

    r = await get_redis()
    t0 = time.monotonic()

    items = list(data.items())
    CHUNK = 500
    written_groups = 0
    written_contracts = 0
    written_bridges = 0

    for i in range(0, len(items), CHUNK):
        batch = items[i : i + CHUNK]
        async with r.pipeline(transaction=False) as pipe:
            for cg_id, g in batch:
                chains = g.get("chains") or {}
                bridges = g.get("bridges") or []
                if not chains:
                    continue

                pipe.delete(f"cg2:group:{cg_id}")
                pipe.hset(f"cg2:group:{cg_id}", mapping=chains)
                pipe.expire(f"cg2:group:{cg_id}", TTL_SEC)
                written_groups += 1

                for chain, addr in chains.items():
                    pipe.setex(f"cg2:contract:{chain}:{addr.lower()}", TTL_SEC, cg_id)
                    written_contracts += 1

                if bridges:
                    pipe.delete(f"cg2:bridges:{cg_id}")
                    pipe.sadd(f"cg2:bridges:{cg_id}", *bridges)
                    pipe.expire(f"cg2:bridges:{cg_id}", TTL_SEC)
                    written_bridges += 1

            await pipe.execute()
        print(f"  {min(i + CHUNK, len(items))}/{len(items)}")

    await r.set("cg2:refreshed_at", str(time.time()))

    print(f"\n--- DONE in {time.monotonic() - t0:.1f}s ---")
    print(f"groups:    {written_groups}")
    print(f"contracts: {written_contracts}")
    print(f"bridges:   {written_bridges}")


if __name__ == "__main__":
    asyncio.run(main())
