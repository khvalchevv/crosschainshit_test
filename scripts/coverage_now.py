"""Quick coverage snapshot: how many tokens are priced vs unfetchable."""
from __future__ import annotations

import asyncio
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import redis.asyncio as r


async def main():
    cli = r.from_url("redis://localhost:6379/1", decode_responses=True)

    # Count all unique (chain, addr) pairs from cg2:contract
    by_chain: Counter = Counter()
    addrs_by_chain: dict[str, list[str]] = {}
    total = 0
    async for k in cli.scan_iter(match="cg2:contract:*", count=2000):
        parts = k.split(":", 3)
        if len(parts) < 4:
            continue
        chain = parts[2]
        addr = parts[3]
        by_chain[chain] += 1
        addrs_by_chain.setdefault(chain, []).append(addr)
        total += 1

    # Check how many have cc2:price set
    priced_by_chain: Counter = Counter()
    async for k in cli.scan_iter(match="cc2:price:*", count=2000):
        parts = k.split(":", 3)
        if len(parts) < 4:
            continue
        priced_by_chain[parts[2]] += 1

    # Per-source known sets
    ds_known   = await cli.scard("cc2:src_known:ds")
    gt_known   = await cli.scard("cc2:src_known:gt")
    okx_known  = await cli.scard("cc2:src_known:okx")

    print("=" * 75)
    print(" COVERAGE NOW")
    print("=" * 75)
    total_priced = sum(priced_by_chain.values())
    print(f"  Total addresses in registry:     {total:>7,}")
    print(f"  With cc2:price set right now:    {total_priced:>7,}  ({total_priced*100//total}%)")
    print(f"  UNFETCHABLE (no price):          {total-total_priced:>7,}  ({(total-total_priced)*100//total}%)")
    print()
    print(f"  Per-source known cache sizes (built up over cycles):")
    print(f"    DexScreener known:  {ds_known:>6,}")
    print(f"    GeckoTerminal:      {gt_known:>6,}")
    print(f"    OKX:                {okx_known:>6,}")
    print(f"    DefiLlama:          (batch — not tracked individually)")

    print()
    print("=" * 75)
    print(" PER-CHAIN COVERAGE (top 25 by unfetchable count)")
    print("=" * 75)
    print(f"  {'CHAIN':<15} {'TOTAL':>7} {'PRICED':>14} {'UNFETCHABLE':>13}")
    print(f"  {'-'*15} {'-'*7} {'-'*14} {'-'*13}")
    rows = []
    for chain, total_c in by_chain.items():
        priced = priced_by_chain.get(chain, 0)
        unfetched = total_c - priced
        rows.append((unfetched, chain, total_c, priced))
    rows.sort(reverse=True)
    for unf, chain, tot, pr in rows[:25]:
        pct = f"{pr*100//tot}%" if tot else "0%"
        print(f"  {chain:<15} {tot:>7,} {pr:>6,}({pct:>4})    {unf:>10,}")

    await cli.close()


asyncio.run(main())
