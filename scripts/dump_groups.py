"""
One-shot: dump all cg:group:* + cg:bridges:* from old Redis into a single JSON
file. Output: data/groups_dump.json — load via load_groups.py to re-hydrate v2.

Reads OLD prefixes (cg:*), so run this against the original bot's Redis.
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

# parent dir on path so `utils` resolves
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import get_redis  # type: ignore


OUT_PATH = Path(__file__).resolve().parent.parent / "data" / "groups_dump.json"


async def main() -> None:
    r = await get_redis()
    t0 = time.monotonic()

    group_keys: list[str] = []
    async for k in r.scan_iter(match="cg:group:*", count=1000):
        group_keys.append(k)
    print(f"found {len(group_keys)} groups, fetching...")

    out: dict[str, dict] = {}
    CHUNK = 1000
    for i in range(0, len(group_keys), CHUNK):
        batch = group_keys[i : i + CHUNK]
        async with r.pipeline(transaction=False) as pipe:
            for k in batch:
                pipe.hgetall(k)
            for k in batch:
                cg_id = k.split(":", 2)[2]
                pipe.smembers(f"cg:bridges:{cg_id}")
            res = await pipe.execute()
        n = len(batch)
        for j, k in enumerate(batch):
            cg_id = k.split(":", 2)[2]
            chains = res[j] or {}
            bridges = sorted(res[n + j] or [])
            if not chains:
                continue
            out[cg_id] = {"chains": chains, "bridges": bridges}
        print(f"  {min(i + CHUNK, len(group_keys))}/{len(group_keys)}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")

    multi = sum(1 for g in out.values() if len(g["chains"]) >= 2)
    contracts = sum(len(g["chains"]) for g in out.values())
    print(f"\n--- DONE in {time.monotonic() - t0:.1f}s ---")
    print(f"groups written:       {len(out)}")
    print(f"  multichain (>=2):   {multi}")
    print(f"  total contracts:    {contracts}")
    print(f"file:                 {OUT_PATH} ({OUT_PATH.stat().st_size / 1024 / 1024:.2f} MB)")


if __name__ == "__main__":
    asyncio.run(main())
