"""
One-shot coverage census. For every unique (chain, addr) in multichain
cg2:group:* (>=2 chains, gnosis excluded), probe DexScreener + GeckoTerminal
+ OKX directly (NO DefiLlama, NO cache) and report exactly which aggregator
prices what, per chain, and how many are dead everywhere.
"""
from __future__ import annotations

import asyncio
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import aiohttp
import redis.asyncio as redis

from utils import get_proxy_manager

# ── chain → source slugs ─────────────────────────────────────────────────
DS_SLUG = {
    "ethereum":"ethereum","bsc":"bsc","polygon":"polygon","arbitrum":"arbitrum",
    "base":"base","optimism":"optimism","avalanche":"avalanche","fantom":"fantom",
    "zksync":"zksync","linea":"linea","blast":"blast","scroll":"scroll",
    "mantle":"mantle","berachain":"berachain","celo":"celo","cronos":"cronos",
    "moonbeam":"moonbeam","metis":"metis","harmony":"harmony","iotex":"iotex",
    "rsk":"rsk","flare":"flare","manta":"manta","taiko":"taiko","plume":"plume",
    "shibarium":"shibarium","zircuit":"zircuit","opbnb":"opbnb","kava":"kava",
    "core":"core","kaia":"kaia","ronin":"ronin","zero":"zero","abstract":"abstract",
    "sonic":"sonic","sophon":"sophon","hyperliquid":"hyperliquid","monad":"monad",
    "pulsechain":"pulsechain","telos":"telos","neon":"neon","oasys":"oasys",
    "bitkub":"bitkub","okex":"okxchain","solana":"solana","sui":"sui",
    "aptos":"aptos","tron":"tron","near":"near","ton":"ton","sei":"sei",
    "cardano":"cardano","filecoin":"filecoin","osmosis":"osmosis",
    "injective":"injective","xrp":"xrp",
}
GT_NET = {
    "ethereum":"eth","bsc":"bsc","polygon":"polygon_pos","arbitrum":"arbitrum",
    "base":"base","optimism":"optimism","avalanche":"avax","fantom":"ftm",
    "zksync":"zksync","linea":"linea","blast":"blast","scroll":"scroll",
    "mantle":"mantle","berachain":"berachain","celo":"celo","cronos":"cro",
    "moonbeam":"glmr","metis":"metis","harmony":"one","iotex":"iotx",
    "flare":"flare","manta":"manta-pacific","taiko":"taiko","zircuit":"zircuit",
    "opbnb":"opbnb","kava":"kava","core":"core","kaia":"kaia","ronin":"ronin",
    "zero":"zero-network","abstract":"abstract","sonic":"sonic",
    "hyperliquid":"hyperliquid","movement":"movement","shibarium":"shibarium",
    "pulsechain":"pulsechain","neon":"neon-evm","oasys":"oasys",
    "bitkub":"bitkub_chain","telos":"tlos","bahamut":"bahamut-mainnet",
    "solana":"solana","sui":"sui-network","aptos":"aptos","tron":"tron",
    "ton":"ton","sei":"sei-network","filecoin":"filecoin","near":"near",
    "stellar":"stellar","hedera":"hedera-hashgraph","xrp":"xrpl",
    "injective":"injective","zilliqa":"zilliqa-evm",
}
OKX_CHAIN = {  # OKX chainName(lower) -> our chain
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


async def ds_probe(session, proxies, queries, found):
    by_chain = defaultdict(list)
    for c, a in queries:
        if c in DS_SLUG:
            by_chain[c].append(a)
    sem = asyncio.Semaphore(800)

    async def one(chain, batch):
        slug = DS_SLUG[chain]
        url = f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch)}"
        async with sem:
            try:
                async with session.get(url, proxy=proxies.next()) as r:
                    if r.status != 200:
                        return
                    body = await r.json(content_type=None)
                    pairs = body.get("pairs") or []
            except Exception:
                return
        aset = {x.lower() for x in batch}
        for p in pairs:
            if (p.get("chainId") or "").lower() != slug:
                continue
            try:
                ba = p["baseToken"]["address"].lower()
                pr = float(p.get("priceUsd") or 0)
                lq = float((p.get("liquidity") or {}).get("usd") or 0)
            except (KeyError, TypeError, ValueError):
                continue
            if pr > 0 and ba in aset and lq >= 500:
                found[(chain, ba)].add("ds")

    tasks = []
    for chain, addrs in by_chain.items():
        for i in range(0, len(addrs), 30):
            tasks.append(one(chain, addrs[i:i+30]))
    await asyncio.gather(*tasks)


async def gt_probe(session, proxies, queries, found):
    by_net = defaultdict(list)
    net2chain = {v: k for k, v in GT_NET.items()}
    for c, a in queries:
        if c in GT_NET:
            by_net[GT_NET[c]].append(a.lower())
    sem = asyncio.Semaphore(700)

    async def one(net, batch):
        url = f"https://api.geckoterminal.com/api/v2/simple/networks/{net}/token_price/{','.join(batch)}"
        async with sem:
            try:
                async with session.get(url, proxy=proxies.next()) as r:
                    if r.status != 200:
                        return
                    body = await r.json(content_type=None)
            except Exception:
                return
        obj = ((body.get("data") or {}).get("attributes") or {}).get("token_prices") or {}
        chain = net2chain.get(net)
        for addr, ps in obj.items():
            try:
                if float(ps) > 0 and chain:
                    found[(chain, addr.lower())].add("gt")
            except (TypeError, ValueError):
                continue

    tasks = []
    for net, addrs in by_net.items():
        for i in range(0, len(addrs), 30):
            tasks.append(one(net, addrs[i:i+30]))
    await asyncio.gather(*tasks)


async def okx_probe(session, proxies, queries, found):
    # dedupe by addr; one search returns all chains the token is on
    addr_chains = defaultdict(set)
    for c, a in queries:
        addr_chains[a.lower()].add(c)
    sem = asyncio.Semaphore(1000)

    async def one(addr):
        url = f"https://web3.okx.com/priapi/v1/dx/market/v2/search?keyword={addr}"
        async with sem:
            try:
                async with session.get(url, proxy=proxies.next()) as r:
                    if r.status != 200:
                        return
                    body = await r.json(content_type=None)
            except Exception:
                return
        for it in (body.get("data") or []):
            try:
                cn = (it.get("chainName") or "").lower()
                pr = float(it.get("price") or 0)
                ra = (it.get("tokenContractAddress") or "").lower()
            except (TypeError, ValueError):
                continue
            our = OKX_CHAIN.get(cn)
            if pr > 0 and our and ra == addr and our in addr_chains[addr]:
                found[(our, addr)].add("okx")

    await asyncio.gather(*[one(a) for a in addr_chains])


async def main():
    cli = redis.from_url("redis://localhost:6379/1", decode_responses=True)
    proxies = get_proxy_manager()

    keys = []
    async for k in cli.scan_iter(match="cg2:group:*", count=2000):
        keys.append(k)
    pairs = set()
    per_chain_total = Counter()
    CH = 1000
    for i in range(0, len(keys), CH):
        b = keys[i:i+CH]
        async with cli.pipeline(transaction=False) as pipe:
            for k in b:
                pipe.hgetall(k)
            res = await pipe.execute()
        for h in res:
            if not h:
                continue
            hh = {c: a for c, a in h.items() if c not in EXCLUDED}
            if len(hh) < 2:
                continue
            for c, a in hh.items():
                t = (c, a.lower())
                if t not in pairs:
                    pairs.add(t)
                    per_chain_total[c] += 1
    queries = list(pairs)
    print(f"Unique (chain,addr) to probe: {len(queries):,}  across {len(per_chain_total)} chains")
    print("Probing DS + GT + OKX (no Llama, no cache)…")

    found = defaultdict(set)
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=1000, limit_per_host=1000),
        timeout=aiohttp.ClientTimeout(total=12),
    ) as s:
        t0 = time.monotonic()
        await asyncio.gather(
            ds_probe(s, proxies, queries, found),
            gt_probe(s, proxies, queries, found),
            okx_probe(s, proxies, queries, found),
        )
        el = time.monotonic() - t0

    src_count = Counter()
    dead = 0
    per_chain_alive = Counter()
    combo = Counter()
    for q in queries:
        srcs = found.get(q, set())
        if not srcs:
            dead += 1
            continue
        per_chain_alive[q[0]] += 1
        for sname in srcs:
            src_count[sname] += 1
        combo[tuple(sorted(srcs))] += 1

    alive = len(queries) - dead
    print(f"\nProbe finished in {el:.0f}s\n")
    print("=" * 70)
    print(" OVERALL")
    print("=" * 70)
    print(f"  Total probed:        {len(queries):>7,}")
    print(f"  Alive (>=1 source):  {alive:>7,}  ({alive*100//len(queries)}%)")
    print(f"  DEAD (no source):    {dead:>7,}  ({dead*100//len(queries)}%)")
    print()
    print("  Per-source coverage (a token can be covered by several):")
    for s_, c in src_count.most_common():
        print(f"    {s_:<6} {c:>7,}")
    print()
    print("  Source combinations:")
    for cmb, c in combo.most_common():
        print(f"    {'+'.join(cmb):<14} {c:>7,}")
    print()
    print("=" * 70)
    print(" PER-CHAIN (sorted by dead count)")
    print("=" * 70)
    print(f"  {'CHAIN':<14}{'TOTAL':>8}{'ALIVE':>8}{'DEAD':>8}{'ALIVE%':>8}")
    rows = []
    for ch, tot in per_chain_total.items():
        al = per_chain_alive.get(ch, 0)
        rows.append((tot - al, ch, tot, al))
    rows.sort(reverse=True)
    for dd, ch, tot, al in rows:
        pct = f"{al*100//tot}%" if tot else "0%"
        print(f"  {ch:<14}{tot:>8,}{al:>8,}{dd:>8,}{pct:>8}")

    await cli.aclose()


if __name__ == "__main__":
    asyncio.run(main())
