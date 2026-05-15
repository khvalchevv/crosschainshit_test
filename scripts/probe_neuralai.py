"""Why didn't NEURALAI auto-alert? Inspect its group, cached prices,
known-set membership, and what each live source returns per chain."""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import aiohttp
import redis.asyncio as r


async def main():
    cli = r.from_url("redis://localhost:6379/1", decode_responses=True)

    # Find the neuralai group
    gid = None
    async for k in cli.scan_iter(match="cg2:group:*neural*", count=1000):
        gid = k
        break
    if not gid:
        # try contract reverse
        async for k in cli.scan_iter(match="cg2:group:*", count=5000):
            if "neural" in k.lower():
                gid = k
                break
    if not gid:
        print("neuralai group not found by name scan")
        await cli.aclose()
        return

    h = await cli.hgetall(gid)
    print(f"GROUP: {gid}")
    for chain, addr in h.items():
        print(f"  {chain:<12} {addr}")

    # Cached price + source-known membership per chain
    known_ds  = await cli.smembers("cc2:src_known:ds")
    known_gt  = await cli.smembers("cc2:src_known:gt")
    known_okx = await cli.smembers("cc2:src_known:okx")

    print("\nCACHED STATE:")
    for chain, addr in h.items():
        a = addr.lower()
        price = await cli.get(f"cc2:price:{chain}:{a}")
        liq   = await cli.get(f"cc2:liq_usd:{chain}:{a}")
        key = f"{chain}:{a}"
        srcs = []
        if key in known_ds:  srcs.append("ds")
        if key in known_gt:  srcs.append("gt")
        if key in known_okx: srcs.append("okx")
        print(f"  {chain:<10} price={price}  liq={liq}  known_by={srcs or '— (llama-only)'}")

    # Live probe DS + DefiLlama per chain
    print("\nLIVE DS PROBE:")
    _slug = {"ethereum":"ethereum","solana":"solana","bsc":"bsc","base":"base",
             "arbitrum":"arbitrum","polygon":"polygon"}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
        for chain, addr in h.items():
            slug = _slug.get(chain)
            if not slug:
                print(f"  {chain:<10} (no DS slug)")
                continue
            url = f"https://api.dexscreener.com/latest/dex/tokens/{addr}"
            try:
                async with s.get(url) as resp:
                    body = await resp.json(content_type=None)
                    pairs = body.get("pairs") or []
            except Exception as e:
                print(f"  {chain:<10} DS ERR {e}")
                continue
            best = None
            for p in pairs:
                if (p.get("chainId") or "").lower() != slug:
                    continue
                try:
                    pr = float(p.get("priceUsd") or 0)
                    lq = float((p.get("liquidity") or {}).get("usd") or 0)
                except (TypeError, ValueError):
                    continue
                if pr > 0 and (best is None or lq > best[1]):
                    best = (pr, lq)
            print(f"  {chain:<10} DS price={best[0] if best else None}  liq={best[1] if best else None}")

        # DefiLlama
        keys = ",".join(f"{c}:{a}" for c, a in h.items())
        try:
            async with s.get(f"https://coins.llama.fi/prices/current/{keys}") as resp:
                d = await resp.json()
            print("\nLIVE DEFILLAMA:")
            for chain, addr in h.items():
                c = d.get("coins", {}).get(f"{chain}:{addr}", {})
                print(f"  {chain:<10} price={c.get('price')}  conf={c.get('confidence')}")
        except Exception as e:
            print(f"  llama ERR {e}")

    await cli.aclose()


asyncio.run(main())
