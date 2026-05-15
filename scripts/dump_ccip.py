"""Dump CCIP-tagged token groups to data/ccip_tokens.json
so we have a local backup even if docs.chain.link changes.

Format mirrors data/wh_wrapped_tokens_normalized.json — re-importable later.
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from utils import close_redis, get_redis, setup_logging


# Map our internal chain names → CG-style platform keys for consistency
# with wh_wrapped_tokens_normalized.json / lz_oft_tokens.json schema.
_CG_PLATFORM_MAP = {
    "ethereum":   "ethereum",
    "bsc":        "binance-smart-chain",
    "polygon":    "polygon-pos",
    "arbitrum":   "arbitrum-one",
    "base":       "base",
    "optimism":   "optimistic-ethereum",
    "avalanche":  "avalanche",
    "fantom":     "fantom",
    "zksync":     "zksync",
    "linea":      "linea",
    "blast":      "blast",
    "scroll":     "scroll",
    "mantle":     "mantle",
    "berachain":  "berachain",
    "celo":       "celo",
    "cronos":     "cronos",
    "moonbeam":   "moonbeam",
    "gnosis":     "xdai",
    "sonic":      "sonic",
    "soneium":    "soneium",
    "unichain":   "unichain",
    "worldchain": "world-chain",
    "mode":       "mode",
    "ink":        "ink",
    "monad":      "monad",
    "abstract":   "abstract",
    "plume":      "plume",
    "sophon":     "sophon",
    "apechain":   "apechain",
    "core":       "core",
    "pulsechain": "pulsechain",
    "lisk":       "lisk",
    "zora":       "zora",
    "hyperliquid": "hyperliquid",
}


async def main() -> None:
    setup_logging()
    out_path = Path(__file__).parent.parent / "data" / "ccip_tokens.json"

    r = await get_redis()
    tokens: dict[str, dict] = {}

    # Collect all groups that have 'ccip' tag (covers both wh-* style and merged-into-existing)
    cursor = 0
    while True:
        cursor, keys = await r.scan(cursor, match="cg2:bridges:*", count=1000)
        for k in keys:
            tags = await r.smembers(k)
            if "ccip" not in tags:
                continue
            gid = k.split(":", 2)[2]
            grp = await r.hgetall(f"cg2:group:{gid}")
            if not grp:
                continue
            # Map internal chain → CG style; drop unmapped (rare)
            platforms = {}
            for chain, addr in grp.items():
                cg_chain = _CG_PLATFORM_MAP.get(chain, chain)
                platforms[cg_chain] = addr.lower() if isinstance(addr, str) else addr
            # Strip ccip- prefix for symbol if present
            sym = gid[5:].upper() if gid.startswith("ccip-") else gid.upper()
            tokens[sym] = {"platforms": platforms}
        if cursor == 0:
            break

    multi = sum(1 for t in tokens.values() if len(t["platforms"]) >= 2)
    payload = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "Chainlink CCIP directory (docs.chain.link/ccip/directory/mainnet)",
        "total_symbols": len(tokens),
        "multi_chain": multi,
        "tokens": tokens,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"\n=== CCIP dump ===")
    print(f"  total symbols:  {len(tokens)}")
    print(f"  multi-chain:    {multi}")
    print(f"  written to:     {out_path}")
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
