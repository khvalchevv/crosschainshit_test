"""
OKX DEX search — undocumented public endpoint that returns price + liquidity +
24h volume + decimals for a token address. No API key needed.

Endpoint: https://web3.okx.com/priapi/v1/dx/market/v2/search?keyword={addr}
One token per request. Webshare datacenter IPs see ~17% rate-limit at high
concurrency, so we read the response either way and silently drop on 429.
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from utils import get_logger, get_proxy_manager

log = get_logger(__name__)

# OKX uses chain names in their response — map their `chainName` (lowercased)
# back to our internal chain id. Only multichain-relevant entries listed.
_OKX_CHAIN_MAP = {
    "ethereum":   "ethereum",
    "bsc":        "bsc",
    "bnb chain":  "bsc",
    "polygon":    "polygon",
    "arbitrum":   "arbitrum",
    "arbitrum one":"arbitrum",
    "base":       "base",
    "optimism":   "optimism",
    "avalanche":  "avalanche",
    "avalanche c-chain": "avalanche",
    "fantom":     "fantom",
    "zksync":     "zksync",
    "zksync era": "zksync",
    "linea":      "linea",
    "blast":      "blast",
    "scroll":     "scroll",
    "mantle":     "mantle",
    "berachain":  "berachain",
    "celo":       "celo",
    "cronos":     "cronos",
    "moonbeam":   "moonbeam",
    "metis":      "metis",
    "manta":      "manta",
    "manta pacific": "manta",
    "taiko":      "taiko",
    "shibarium":  "shibarium",
    "zircuit":    "zircuit",
    "opbnb":      "opbnb",
    "kava":       "kava",
    "kaia":       "kaia",
    "ronin":      "ronin",
    "abstract":   "abstract",
    "sonic":      "sonic",
    "sui":        "sui",
    "solana":     "solana",
    "ton":        "ton",
    "tron":       "tron",
    "aptos":      "aptos",
}

PARALLEL = 1000


class OKXClient:
    def __init__(self) -> None:
        self._proxies = get_proxy_manager()
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=1000, limit_per_host=1000),
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def fetch_prices(
        self,
        queries: list[tuple[str, str]],
    ) -> dict[tuple[str, str], dict]:
        """Returns {(chain, addr_lower): {"price": ..., "liq": ..., "vol_h24": ...}}.

        Note: OKX search by address is chain-agnostic — it returns ALL chains
        the token deploys to. We pick the entry whose chainName matches the
        chain the caller asked for.
        """
        if not queries:
            return {}

        # Dedupe by addr (one search per unique addr regardless of chain).
        # Map addr -> set of chains we want from caller.
        addr_to_chains: dict[str, set[str]] = {}
        for chain, addr in queries:
            addr_to_chains.setdefault(addr.lower(), set()).add(chain)

        sem = asyncio.Semaphore(PARALLEL)
        tasks = [self._fetch_one(sem, addr) for addr in addr_to_chains]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        out: dict[tuple[str, str], dict] = {}
        for addr, res in zip(addr_to_chains, results):
            if isinstance(res, Exception) or not res:
                continue
            wanted_chains = addr_to_chains[addr]
            for chain_name, info in res.items():
                our_chain = _OKX_CHAIN_MAP.get(chain_name)
                if our_chain and our_chain in wanted_chains:
                    out[(our_chain, addr)] = info
        return out

    async def _fetch_one(
        self, sem: asyncio.Semaphore, addr: str,
    ) -> dict[str, dict] | None:
        url = f"https://web3.okx.com/priapi/v1/dx/market/v2/search?keyword={addr}"
        async with sem:
            proxy = self._proxies.next()
            session = await self._get_session()
            kwargs: dict[str, Any] = {"proxy": proxy} if proxy else {}
            try:
                async with session.get(url, **kwargs) as resp:
                    if resp.status != 200:
                        return None
                    body = await resp.json(content_type=None)
            except Exception:
                return None

        # OKX returns `data` as a list of token entries (one per chain the
        # token is deployed on).
        items = body.get("data") or []
        if not isinstance(items, list):
            return None

        out: dict[str, dict] = {}
        for it in items:
            try:
                chain_name = (it.get("chainName") or "").lower()
                price = float(it.get("price") or 0)
                if price <= 0:
                    continue
                liq = float(it.get("liquidity") or 0)
                vol = float(it.get("volume") or 0)
                addr_resp = (it.get("tokenContractAddress") or "").lower()
            except (TypeError, ValueError):
                continue
            if addr_resp and addr_resp != addr:
                continue
            existing = out.get(chain_name)
            if existing is None or liq > existing.get("liq", 0):
                out[chain_name] = {"price": price, "liq": liq, "vol_h24": vol}
        return out
