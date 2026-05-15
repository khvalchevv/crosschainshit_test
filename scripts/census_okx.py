"""Clean OKX-only coverage pass (no DS/GT competing for the proxy pool)."""
import asyncio, sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import aiohttp, redis.asyncio as redis
from utils import get_proxy_manager

OKX_CHAIN = {
    "ethereum":"ethereum","bnb chain":"bsc","bsc":"bsc","polygon":"polygon",
    "arbitrum":"arbitrum","arbitrum one":"arbitrum","base":"base",
    "optimism":"optimism","avalanche":"avalanche","avalanche c-chain":"avalanche",
    "fantom":"fantom","zksync era":"zksync","linea":"linea","blast":"blast",
    "scroll":"scroll","mantle":"mantle","berachain":"berachain","celo":"celo",
    "cronos":"cronos","metis":"metis","manta pacific":"manta","taiko":"taiko",
    "shibarium":"shibarium","zircuit":"zircuit","opbnb":"opbnb","kava":"kava",
    "kaia":"kaia","ronin":"ronin","abstract":"abstract","sonic":"sonic",
    "sui":"sui","solana":"solana","ton":"ton","tron":"tron","aptos":"aptos",
}
EXCLUDED = {"gnosis"}

async def main():
    cli = redis.from_url("redis://localhost:6379/1", decode_responses=True)
    proxies = get_proxy_manager()
    keys=[]
    async for k in cli.scan_iter(match="cg2:group:*", count=2000): keys.append(k)
    addr_chains=defaultdict(set)
    for i in range(0,len(keys),1000):
        b=keys[i:i+1000]
        async with cli.pipeline(transaction=False) as p:
            for k in b: p.hgetall(k)
            res=await p.execute()
        for h in res:
            if not h: continue
            hh={c:a for c,a in h.items() if c not in EXCLUDED}
            if len(hh)<2: continue
            for c,a in hh.items(): addr_chains[a.lower()].add(c)
    print(f"Unique addrs for OKX: {len(addr_chains):,}")
    found=defaultdict(set)
    sem=asyncio.Semaphore(400)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=400,limit_per_host=400),timeout=aiohttp.ClientTimeout(total=12)) as s:
        async def one(addr):
            url=f"https://web3.okx.com/priapi/v1/dx/market/v2/search?keyword={addr}"
            async with sem:
                try:
                    async with s.get(url,proxy=proxies.next()) as r:
                        if r.status!=200: return
                        body=await r.json(content_type=None)
                except Exception: return
            for it in (body.get("data") or []):
                try:
                    cn=(it.get("chainName") or "").lower()
                    pr=float(it.get("price") or 0)
                    ra=(it.get("tokenContractAddress") or "").lower()
                except (TypeError,ValueError): continue
                our=OKX_CHAIN.get(cn)
                if pr>0 and our and ra==addr and our in addr_chains[addr]:
                    found[(our,addr)].add("okx")
        import time; t0=time.monotonic()
        await asyncio.gather(*[one(a) for a in addr_chains])
        el=time.monotonic()-t0
    pc=Counter()
    for (ch,_),_ in found.items(): pc[ch]+=1
    print(f"OKX priced (chain,addr): {len(found):,}  in {el:.0f}s")
    print("Top chains by OKX coverage:")
    for ch,c in pc.most_common(25): print(f"  {ch:<14}{c:>7,}")
    await cli.aclose()

asyncio.run(main())
