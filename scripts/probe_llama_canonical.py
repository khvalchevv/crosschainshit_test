"""Sample real multichain groups, query DefiLlama per-chain, and measure how
often Llama returns an IDENTICAL price across chains (canonical leak) — those
are tokens whose cross-chain spread we'd be blind to if Llama is the source."""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import aiohttp
import redis.asyncio as r

_DL = {
    "ethereum":"ethereum","bsc":"bsc","polygon":"polygon","arbitrum":"arbitrum",
    "base":"base","optimism":"optimism","avalanche":"avax","fantom":"fantom",
    "zksync":"era","linea":"linea","blast":"blast","scroll":"scroll",
    "mantle":"mantle","berachain":"berachain","celo":"celo","cronos":"cronos",
    "sonic":"sonic","solana":"solana","sui":"sui","aptos":"aptos","tron":"tron",
}


async def main():
    cli = r.from_url("redis://localhost:6379/1", decode_responses=True)

    # Collect multichain groups with >=2 DL-mappable chains
    groups = []
    async for k in cli.scan_iter(match="cg2:group:*", count=2000):
        h = await cli.hgetall(k)
        chains = {c: a for c, a in h.items() if c in _DL}
        if len(chains) >= 2:
            groups.append((k.split(":", 2)[-1], chains))
        if len(groups) >= 400:
            break

    import random
    sample = random.sample(groups, min(250, len(groups)))

    canonical_leak = 0     # Llama identical price across ALL chains in group
    real_spread = 0        # Llama itself shows >2% divergence
    partial = 0            # mixed
    no_data = 0
    leak_examples = []

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as s:
        # batch llama calls (100 keys per request)
        all_keys = []
        key_owner = {}
        for cg_id, chains in sample:
            for chain, addr in chains.items():
                key = f"{_DL[chain]}:{addr.lower()}"
                all_keys.append(key)
                key_owner[key] = (cg_id, chain)

        prices = {}
        for i in range(0, len(all_keys), 100):
            batch = all_keys[i:i+100]
            url = "https://coins.llama.fi/prices/current/" + ",".join(batch)
            try:
                async with s.get(url) as resp:
                    d = await resp.json()
            except Exception:
                continue
            for k, v in (d.get("coins") or {}).items():
                if isinstance(v, dict) and v.get("price"):
                    prices[k] = float(v["price"])

    for cg_id, chains in sample:
        vals = []
        for chain, addr in chains.items():
            key = f"{_DL[chain]}:{addr.lower()}"
            if key in prices:
                vals.append(prices[key])
        if len(vals) < 2:
            no_data += 1
            continue
        mn, mx = min(vals), max(vals)
        if mn <= 0:
            no_data += 1
            continue
        div = (mx - mn) / mn * 100
        if div < 0.01:
            canonical_leak += 1
            if len(leak_examples) < 12:
                leak_examples.append((cg_id, len(vals), round(vals[0], 6)))
        elif div > 2:
            real_spread += 1
        else:
            partial += 1

    n = canonical_leak + real_spread + partial + no_data
    print(f"Sampled {n} multichain groups (>=2 DL chains)\n")
    print(f"  Canonical leak (identical price all chains): {canonical_leak:>4}  ({canonical_leak*100//max(n,1)}%)")
    print(f"  Llama shows real divergence >2%:             {real_spread:>4}  ({real_spread*100//max(n,1)}%)")
    print(f"  Minor divergence 0-2%:                       {partial:>4}")
    print(f"  No/insufficient data:                        {no_data:>4}")
    print()
    print("Canonical-leak examples (cg_id, #chains identical, price):")
    for cg, nc, pr in leak_examples:
        print(f"  {cg:<32} {nc} chains @ ${pr}")

    await cli.aclose()


asyncio.run(main())
