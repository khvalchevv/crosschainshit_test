"""Sample unfetched (chain, addr) pairs across major chains for manual checks."""
import asyncio
import sys
from pathlib import Path
import random

sys.path.insert(0, str(Path(__file__).parent.parent))
import redis.asyncio as r


EXPLORERS = {
    "ethereum":   "https://etherscan.io/token/",
    "bsc":        "https://bscscan.com/token/",
    "polygon":    "https://polygonscan.com/token/",
    "arbitrum":   "https://arbiscan.io/token/",
    "base":       "https://basescan.org/token/",
    "optimism":   "https://optimistic.etherscan.io/token/",
    "avalanche":  "https://snowtrace.io/token/",
    "solana":     "https://solscan.io/token/",
    "blast":      "https://blastscan.io/token/",
    "linea":      "https://lineascan.build/token/",
    "scroll":     "https://scrollscan.com/token/",
    "mantle":     "https://explorer.mantle.xyz/token/",
    "berachain":  "https://berascan.com/token/",
    "sonic":      "https://sonicscan.org/token/",
    "hyperliquid":"https://hyperliquid.cloud.blockscout.com/token/",
    "fantom":     "https://ftmscan.com/token/",
}

# How many samples per chain
SAMPLES_PER_CHAIN = 3
TARGET_CHAINS = ["ethereum", "bsc", "base", "arbitrum", "polygon", "solana",
                 "avalanche", "optimism", "blast", "fantom"]


async def main():
    cli = r.from_url("redis://localhost:6379/1", decode_responses=True)

    # Collect unfetched (chain, addr) pairs grouped by chain
    by_chain: dict[str, list[tuple[str, str]]] = {}
    async for k in cli.scan_iter(match="cg2:contract:*", count=2000):
        parts = k.split(":", 3)
        if len(parts) < 4:
            continue
        chain = parts[2]
        addr = parts[3]
        if chain not in TARGET_CHAINS:
            continue
        # check price exists
        has_price = await cli.exists(f"cc2:price:{chain}:{addr}")
        if not has_price:
            cg_id = await cli.get(k)
            by_chain.setdefault(chain, []).append((addr, cg_id))

    for chain in TARGET_CHAINS:
        items = by_chain.get(chain, [])
        if not items:
            continue
        sample = random.sample(items, min(SAMPLES_PER_CHAIN, len(items)))
        explorer = EXPLORERS.get(chain, "")
        print(f"\n[{chain.upper()}]  {len(items)} unfetched")
        for addr, cg_id in sample:
            link = f"{explorer}{addr}" if explorer else addr
            print(f"  {cg_id:<35} {link}")

    await cli.aclose()


asyncio.run(main())
