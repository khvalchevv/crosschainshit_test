"""For tokens currently priced only by Llama, probe if DS/GT/OKX would
actually return data if asked. Tells us whether discovery is broken or
those 8800 tokens really are Llama-only."""
import asyncio
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import aiohttp
import redis.asyncio as r


_DS_SLUG = {
    "ethereum":"ethereum","bsc":"bsc","polygon":"polygon","arbitrum":"arbitrum",
    "base":"base","optimism":"optimism","avalanche":"avalanche","fantom":"fantom",
    "blast":"blast","linea":"linea","scroll":"scroll","mantle":"mantle",
    "berachain":"berachain","sonic":"sonic","solana":"solana","sui":"sui",
    "hyperliquid":"hyperliquid","zksync":"zksync","moonbeam":"moonbeam",
    "harmony":"harmony","cronos":"cronos","celo":"celo","metis":"metis",
}


async def main():
    cli = r.from_url("redis://localhost:6379/1", decode_responses=True)

    # Read known sets
    known_ds  = await cli.smembers("cc2:src_known:ds")
    known_gt  = await cli.smembers("cc2:src_known:gt")
    known_okx = await cli.smembers("cc2:src_known:okx")
    known = known_ds | known_gt | known_okx

    # Find tokens in cc2:price:* that are NOT in any per-chain known set
    # (i.e., currently priced only by Llama).
    llama_only: list[tuple[str, str]] = []
    async for k in cli.scan_iter(match="cc2:price:*", count=2000):
        parts = k.split(":", 3)
        if len(parts) < 4:
            continue
        chain, addr = parts[2], parts[3]
        key = f"{chain}:{addr}"
        if key in known:
            continue
        # Restrict to chains DS supports for fair test
        if chain not in _DS_SLUG:
            continue
        llama_only.append((chain, addr))

    print(f"Llama-only tokens (not in any per-chain known set): {len(llama_only):,}")
    print(f"Sampling 100 random for DS check...")
    print()

    sample = random.sample(llama_only, min(100, len(llama_only)))

    # Probe DS for each
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=50),
        timeout=aiohttp.ClientTimeout(total=8),
    ) as s:
        ds_hits = 0
        ds_no_pair = 0
        ds_err = 0
        chain_breakdown: dict[str, dict] = {}
        for chain, addr in sample:
            slug = _DS_SLUG[chain]
            url = f"https://api.dexscreener.com/latest/dex/tokens/{addr}"
            try:
                async with s.get(url) as resp:
                    if resp.status != 200:
                        ds_err += 1
                        continue
                    body = await resp.json(content_type=None)
                    pairs = body.get("pairs") or []
            except Exception:
                ds_err += 1
                continue

            cb = chain_breakdown.setdefault(chain, {"hit":0,"miss":0})
            found = False
            for p in pairs:
                if (p.get("chainId") or "").lower() != slug:
                    continue
                try:
                    price = float(p.get("priceUsd") or 0)
                    liq = float((p.get("liquidity") or {}).get("usd") or 0)
                except (TypeError, ValueError):
                    continue
                if price > 0 and liq > 500:
                    found = True
                    break
            if found:
                ds_hits += 1
                cb["hit"] += 1
            else:
                ds_no_pair += 1
                cb["miss"] += 1

    print(f"DS results for sample of {len(sample)}:")
    print(f"  Has pool with liq>$500:  {ds_hits:>4}")
    print(f"  No pool / dead pool:     {ds_no_pair:>4}")
    print(f"  Errors:                  {ds_err:>4}")
    print()
    print("Per chain:")
    for chain, cb in sorted(chain_breakdown.items(), key=lambda x: -x[1]["hit"]):
        total = cb["hit"] + cb["miss"]
        print(f"  {chain:<12} {cb['hit']:>3}/{total:<3}")

    await cli.aclose()


asyncio.run(main())
