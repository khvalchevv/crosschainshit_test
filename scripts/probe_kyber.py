"""KyberSwap coverage probe. Of multichain tokens on KS-supported chains that
DS+GT do NOT already cover (the real gap), how many can KyberSwap quote, and
at what price (in_usd ≈ per-token USD for 18-dec tokens)."""
import asyncio
import random
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import redis.asyncio as redis
from core.kyberswap import KyberSwapClient, _KS_CHAIN_MAP

EXCLUDED = {"gnosis"}
SAMPLE = 2500


async def main():
    cli = redis.from_url("redis://localhost:6379/1", decode_responses=True)
    ks_chains = set(_KS_CHAIN_MAP.keys())

    known = set()
    for s in ("ds", "gt"):
        known |= set(await cli.smembers(f"cc2:src_known:{s}") or set())

    keys = []
    async for k in cli.scan_iter(match="cg2:group:*", count=2000):
        keys.append(k)

    gap = []          # tokens on KS chains NOT covered by DS/GT
    on_ks_total = 0
    for i in range(0, len(keys), 1000):
        b = keys[i:i+1000]
        async with cli.pipeline(transaction=False) as p:
            for k in b:
                p.hgetall(k)
            res = await p.execute()
        for h in res:
            if not h:
                continue
            hh = {c: a for c, a in h.items() if c not in EXCLUDED}
            if len(hh) < 2:
                continue
            for c, a in hh.items():
                if c in ks_chains:
                    on_ks_total += 1
                    if f"{c}:{a.lower()}" not in known:
                        gap.append((c, a.lower()))

    gap = list({(c, a) for c, a in gap})
    print(f"Tokens on KS chains total: {on_ks_total:,}")
    print(f"Gap (KS-chain, NOT in DS/GT known): {len(gap):,}")
    sample = random.sample(gap, min(SAMPLE, len(gap)))
    print(f"Probing KyberSwap for {len(sample):,} sampled gap tokens…\n")

    ks = KyberSwapClient()
    hit = 0
    per_chain_hit = Counter()
    per_chain_tot = Counter()
    examples = []
    t0 = time.monotonic()

    async def one(chain, addr):
        nonlocal hit
        per_chain_tot[chain] += 1
        try:
            q = await ks.quote(chain, addr)
        except Exception:
            q = None
        if q and q.get("in_usd", 0) > 0:
            hit += 1
            per_chain_hit[chain] += 1
            if len(examples) < 15:
                examples.append((chain, addr, q["in_usd"], round(q["slippage_pct"], 2)))

    await asyncio.gather(*[one(c, a) for c, a in sample])
    el = time.monotonic() - t0
    await ks.close()

    print(f"Probe done in {el:.0f}s\n")
    print("=" * 60)
    pct = hit * 100 // max(len(sample), 1)
    print(f"  KS quoted: {hit:,}/{len(sample):,}  ({pct}%)")
    print(f"  → extrapolated full gap coverage: ~{len(gap)*pct//100:,} tokens")
    print("=" * 60)
    print("\nPer-chain hit rate (sampled):")
    for ch in sorted(per_chain_tot, key=lambda c: -per_chain_tot[c]):
        t = per_chain_tot[ch]
        hh = per_chain_hit.get(ch, 0)
        print(f"  {ch:<12} {hh:>4}/{t:<4}  ({hh*100//t if t else 0}%)")
    print("\nExample quotes (chain, addr, in_usd, slippage%):")
    for ch, a, p, sl in examples:
        print(f"  {ch:<10} {a[:14]}…  ${p:>12,.4f}  slip={sl}%")

    await cli.aclose()


asyncio.run(main())
