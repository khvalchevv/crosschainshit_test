"""DS-only census, NO liq filter — true 'DexScreener knows this token at all'.
Also splits by token prefix to show how much of the 22k is bridge-wrapper/RWA junk."""
import asyncio, sys, time
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import aiohttp, redis.asyncio as redis
from utils import get_proxy_manager

DS_SLUG = {"ethereum":"ethereum","bsc":"bsc","polygon":"polygon","arbitrum":"arbitrum",
"base":"base","optimism":"optimism","avalanche":"avalanche","fantom":"fantom",
"zksync":"zksync","linea":"linea","blast":"blast","scroll":"scroll","mantle":"mantle",
"berachain":"berachain","celo":"celo","cronos":"cronos","moonbeam":"moonbeam",
"metis":"metis","harmony":"harmony","iotex":"iotex","rsk":"rsk","flare":"flare",
"manta":"manta","taiko":"taiko","plume":"plume","shibarium":"shibarium",
"zircuit":"zircuit","opbnb":"opbnb","kava":"kava","core":"core","kaia":"kaia",
"ronin":"ronin","abstract":"abstract","sonic":"sonic","hyperliquid":"hyperliquid",
"monad":"monad","pulsechain":"pulsechain","telos":"telos","solana":"solana",
"sui":"sui","aptos":"aptos","tron":"tron","sei":"sei"}
EXCLUDED={"gnosis"}

async def main():
    cli=redis.from_url("redis://localhost:6379/1",decode_responses=True)
    proxies=get_proxy_manager()
    keys=[]
    async for k in cli.scan_iter(match="cg2:group:*",count=2000): keys.append(k)
    pairs=set(); cgid_of={}
    for i in range(0,len(keys),1000):
        b=keys[i:i+1000]
        async with cli.pipeline(transaction=False) as p:
            for k in b: p.hgetall(k)
            res=await p.execute()
        for k,h in zip(b,res):
            if not h: continue
            hh={c:a for c,a in h.items() if c not in EXCLUDED}
            if len(hh)<2: continue
            cg=k.split(":",2)[-1]
            for c,a in hh.items():
                t=(c,a.lower()); pairs.add(t); cgid_of[t]=cg
    queries=[q for q in pairs if q[0] in DS_SLUG]
    print(f"Probing DS (no liq filter): {len(queries):,} of {len(pairs):,} (DS-supported chains)")
    found=set(); pricedliq=set()
    by_chain=defaultdict(list)
    for c,a in queries: by_chain[c].append(a)
    sem=asyncio.Semaphore(600)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=600),timeout=aiohttp.ClientTimeout(total=12)) as s:
        async def one(chain,batch):
            slug=DS_SLUG[chain]
            url=f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch)}"
            async with sem:
                try:
                    async with s.get(url,proxy=proxies.next()) as r:
                        if r.status!=200: return
                        body=await r.json(content_type=None)
                except Exception: return
            aset={x.lower() for x in batch}
            for p in (body.get("pairs") or []):
                if (p.get("chainId") or "").lower()!=slug: continue
                try:
                    ba=p["baseToken"]["address"].lower()
                    pr=float(p.get("priceUsd") or 0)
                    lq=float((p.get("liquidity") or {}).get("usd") or 0)
                except (KeyError,TypeError,ValueError): continue
                if pr>0 and ba in aset:
                    found.add((chain,ba))
                    if lq>=500: pricedliq.add((chain,ba))
        tasks=[]
        for chain,addrs in by_chain.items():
            for i in range(0,len(addrs),30): tasks.append(one(chain,addrs[i:i+30]))
        t0=time.monotonic(); await asyncio.gather(*tasks); el=time.monotonic()-t0
    # prefix breakdown of dead
    dead=[q for q in queries if q not in found]
    pref=Counter()
    for q in dead:
        cg=cgid_of.get(q,"")
        p=cg.split("-")[0] if "-" in cg else cg
        pref[p]+=1
    print(f"\nDS probe {el:.0f}s")
    print(f"  DS has ANY pool (any liq):  {len(found):,}  ({len(found)*100//len(queries)}%)")
    print(f"  DS pool with liq>=$500:     {len(pricedliq):,}")
    print(f"  DS knows nothing:           {len(dead):,}")
    print("\nDead-token cg_id prefixes (top 20) — shows the junk:")
    for pr,c in pref.most_common(20): print(f"  {pr:<22}{c:>6,}")
    await cli.aclose()
asyncio.run(main())
