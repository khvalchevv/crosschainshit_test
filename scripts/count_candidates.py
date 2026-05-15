"""Count how many multichain tokens currently have a spread above threshold."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config import get_thresholds
from utils import close_redis, get_redis


async def main() -> None:
    cfg = get_thresholds()["arbitrage"]
    min_p = cfg["min_profit_percent"]
    max_p = cfg["max_profit_percent"]

    r = await get_redis()

    # Load all groups
    group_keys = [k async for k in r.scan_iter(match="cg2:group:*", count=1000)]
    groups: dict[str, dict[str, str]] = {}
    for i in range(0, len(group_keys), 1000):
        batch = group_keys[i : i + 1000]
        async with r.pipeline(transaction=False) as pipe:
            for k in batch:
                pipe.hgetall(k)
            results = await pipe.execute()
        for k, h in zip(batch, results):
            if h and len(h) >= 2:
                groups[k.split(":", 2)[-1]] = h

    # Load all prices
    contracts: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for g in groups.values():
        for chain, addr in g.items():
            t = (chain, addr.lower())
            if t not in seen:
                seen.add(t)
                contracts.append(t)

    prices: dict[tuple[str, str], float | None] = {}
    for i in range(0, len(contracts), 1000):
        batch = contracts[i : i + 1000]
        async with r.pipeline(transaction=False) as pipe:
            for c, a in batch:
                pipe.get(f"cc2:price:{c}:{a}")
            results = await pipe.execute()
        for (c, a), v in zip(batch, results):
            try:
                prices[(c, a)] = float(v) if v else None
            except (TypeError, ValueError):
                prices[(c, a)] = None

    # Count candidates + distribution
    buckets = {"<6%": 0, "6-10%": 0, "10-20%": 0, "20-50%": 0, "50-100%": 0, "100%+": 0}
    top = []
    for cg_id, g in groups.items():
        ps = []
        for chain, addr in g.items():
            p = prices.get((chain, addr.lower()))
            if p and p > 0:
                ps.append((p, chain))
        if len(ps) < 2:
            continue
        ps.sort()
        cheap_p, cheap_c = ps[0]
        exp_p, exp_c = ps[-1]
        sp = (exp_p - cheap_p) / cheap_p * 100

        if sp < 6: buckets["<6%"] += 1
        elif sp < 10: buckets["6-10%"] += 1
        elif sp < 20: buckets["10-20%"] += 1
        elif sp < 50: buckets["20-50%"] += 1
        elif sp < 100: buckets["50-100%"] += 1
        else: buckets["100%+"] += 1

        if min_p <= sp <= max_p:
            top.append((sp, cg_id, cheap_c, exp_c))

    top.sort(reverse=True)

    print(f"Total multichain groups with ≥2 prices: {sum(buckets.values())}")
    print(f"Min profit threshold: {min_p}%, Max: {max_p}%")
    print()
    print("Spread distribution:")
    for k, v in buckets.items():
        print(f"  {k:<10} {v:>5,}")
    print()
    print(f"Candidates for alert (within thresholds): {len(top)}")
    print("\nTop 20 by spread:")
    for sp, cg_id, c1, c2 in top[:20]:
        print(f"  {sp:>7.2f}%  {cg_id:<30}  {c1} -> {c2}")

    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
