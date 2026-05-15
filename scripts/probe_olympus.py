"""Probe what DefiLlama, DexScreener, GeckoTerminal, OKX return for OLYMPUS
across chains — debug the 'same canonical price for all chains' issue."""
import asyncio
import aiohttp


ADDRS = {
    "ethereum":  "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5",
    "arbitrum":  "0xf0cb2dc0db5e6c66b9a70ac27b06b878da017028",
    "base":      "0x060cb087a9730e13aa191f31a6d86bff8dfcdcc0",
    "polygon":   "0xfa49101d56734af877aa312a6a40f634d4e3729d",
    "optimism":  "0x060cb087a9730e13aa191f31a6d86bff8dfcdcc0",
    "berachain": "0x18878df23e2a36f81e820e4b47b4a40576d3159c",
}


async def llama(s):
    keys = ",".join(f"{c}:{a}" for c, a in ADDRS.items())
    url = f"https://coins.llama.fi/prices/current/{keys}"
    async with s.get(url) as r:
        d = await r.json()
    print("=== DefiLlama ===")
    for chain, addr in ADDRS.items():
        c = d.get("coins", {}).get(f"{chain}:{addr}", {})
        print(f"  {chain:<12} price={c.get('price')}  conf={c.get('confidence')}  sym={c.get('symbol')}")


async def dexscreener(s):
    print("\n=== DexScreener ===")
    for chain, addr in ADDRS.items():
        url = f"https://api.dexscreener.com/latest/dex/tokens/{addr}"
        try:
            async with s.get(url) as r:
                d = await r.json()
        except Exception as e:
            print(f"  {chain:<12} ERR: {e}")
            continue
        pairs = d.get("pairs") or []
        # filter to chain
        slug_map = {"arbitrum":"arbitrum","ethereum":"ethereum","polygon":"polygon",
                    "base":"base","optimism":"optimism","berachain":"berachain"}
        slug = slug_map.get(chain)
        for p in pairs:
            if (p.get("chainId") or "").lower() == slug:
                price = p.get("priceUsd")
                liq = (p.get("liquidity") or {}).get("usd", 0)
                print(f"  {chain:<12} price={price}  liq=${liq}  pair={p.get('dexId')}/{p.get('pairAddress', '')[:8]}")
                break
        else:
            print(f"  {chain:<12} no pair found")


async def main():
    async with aiohttp.ClientSession() as s:
        await llama(s)
        await dexscreener(s)


asyncio.run(main())
