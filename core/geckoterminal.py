"""
GeckoTerminal — last-resort price source for the smallest chains.
Covers ~200 networks including Oasys, Hyperliquid, Monad, Ronin, Kaia,
Pulsechain, Bitkub, and many more that neither DS nor DefiLlama have.

Free, no API key (30 req/min per IP — bypassed via proxy rotation).
Endpoint: /api/v2/simple/networks/{network}/token_price/{addresses}
         (up to 30 comma-separated addresses per call, same network)
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from utils import get_logger, get_proxy_manager, get_redis, norm_addr

log = get_logger(__name__)

# Cache TTL — GT mostly serves non-EVM/small-EVM chains where prices move
# slow. One cycle's worth of caching gives ~6× fewer requests, which keeps
# us under GT's per-IP rate limits even at high proxy churn. Verifier still
# re-queries DS at alert time so cached price doesn't poison alerts.
_CACHE_TTL_SEC = 45

# Our internal chain name → GeckoTerminal network id
_GT_NETWORK_MAP = {
    # Major EVM
    "ethereum":     "eth",
    "bsc":          "bsc",
    "polygon":      "polygon_pos",
    "arbitrum":     "arbitrum",
    "base":         "base",
    "optimism":     "optimism",
    "avalanche":    "avax",
    "fantom":       "ftm",
    "zksync":       "zksync",
    "linea":        "linea",
    "blast":        "blast",
    "scroll":       "scroll",
    "mantle":       "mantle",
    "berachain":    "berachain",
    "celo":         "celo",
    "cronos":       "cro",
    "moonbeam":     "glmr",
    "metis":        "metis",
    "harmony":      "one",
    "iotex":        "iotx",
    "flare":        "flare",
    "manta":        "manta-pacific",
    "taiko":        "taiko",
    "zircuit":      "zircuit",
    "opbnb":        "opbnb",
    "kava":         "kava",
    "core":         "core",
    "kaia":         "kaia",
    "ronin":        "ronin",
    "zero":         "zero-network",
    "abstract":     "abstract",
    "sonic":        "sonic",
    "hyperliquid":  "hyperliquid",
    "movement":     "movement",
    "shibarium":    "shibarium",
    "pulsechain":   "pulsechain",
    "neon":         "neon-evm",
    "oasys":        "oasys",
    "bitkub":       "bitkub_chain",
    "telos":        "tlos",
    "bahamut":      "bahamut-mainnet",
    # Non-EVM
    "solana":       "solana",
    "sui":          "sui-network",
    "aptos":        "aptos",
    "tron":         "tron",
    "ton":          "ton",
    "sei":          "sei-network",
    "filecoin":     "filecoin",
    "near":         "near",  # GeckoTerminal coverage of NEAR may be partial
    "stellar":      "stellar",
    "hedera":       "hedera-hashgraph",
    "xrp":          "xrpl",
    "injective":    "injective",
    "zilliqa":      "zilliqa-evm",
}

BATCH_SIZE = 30          # GeckoTerminal max per call
PARALLEL = 700           # use most of 1000-proxy pool


class GeckoTerminalClient:
    def __init__(self) -> None:
        self._proxies = get_proxy_manager()
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=700, limit_per_host=300),
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def fetch_prices(
        self,
        queries: list[tuple[str, str]],  # [(chain, addr), ...]
    ) -> dict[tuple[str, str], float]:
        """Returns {(chain, addr_lower): price} for resolved entries.

        Two-phase: first read cc2:gt_cache:* (TTL 20s) for any (chain, addr)
        cached from a previous cycle. Only HTTP-fetch what's missing. After
        fetch, write new prices back to cache.
        """
        if not queries:
            return {}

        r = await get_redis()
        out: dict[tuple[str, str], float] = {}

        # ── Cache read ────────────────────────────────────────────────────
        cache_keys = [f"cc2:gt_cache:{c}:{norm_addr(a)}" for c, a in queries]
        if cache_keys:
            async with r.pipeline(transaction=False) as pipe:
                for k in cache_keys:
                    pipe.get(k)
                cached = await pipe.execute()
            for (chain, addr), v in zip(queries, cached):
                if v is None:
                    continue
                try:
                    out[(chain, norm_addr(addr))] = float(v)
                except (TypeError, ValueError):
                    continue

        # ── Determine misses ──────────────────────────────────────────────
        misses = [(c, a) for c, a in queries if (c, norm_addr(a)) not in out]
        if not misses:
            return out

        # Group misses by GT network id
        by_net: dict[str, list[str]] = {}
        for chain, addr in misses:
            net = _GT_NETWORK_MAP.get(chain)
            if not net:
                continue
            by_net.setdefault(net, []).append(norm_addr(addr))

        if not by_net:
            return out

        net_to_chain = {v: k for k, v in _GT_NETWORK_MAP.items()}

        sem = asyncio.Semaphore(PARALLEL)
        tasks = []
        for net, addrs in by_net.items():
            for i in range(0, len(addrs), BATCH_SIZE):
                batch = addrs[i : i + BATCH_SIZE]
                tasks.append(self._fetch_batch(sem, net, batch))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        fresh: dict[tuple[str, str], float] = {}
        for res in results:
            if isinstance(res, Exception):
                continue
            for (net, addr), price in res.items():
                chain = net_to_chain.get(net)
                if chain:
                    fresh[(chain, addr)] = price

        # ── Cache write (only freshly fetched, not stale repeats) ────────
        if fresh:
            async with r.pipeline(transaction=False) as pipe:
                for (chain, addr), price in fresh.items():
                    pipe.setex(f"cc2:gt_cache:{chain}:{addr}",
                               _CACHE_TTL_SEC, str(price))
                await pipe.execute()
            out.update(fresh)

        return out

    async def _fetch_batch(
        self,
        sem: asyncio.Semaphore,
        network: str,
        addresses: list[str],
    ) -> dict[tuple[str, str], float]:
        url = (
            f"https://api.geckoterminal.com/api/v2/simple/networks/"
            f"{network}/token_price/{','.join(addresses)}"
        )
        async with sem:
            proxy = self._proxies.next()
            session = await self._get_session()
            kwargs: dict[str, Any] = {"proxy": proxy} if proxy else {}
            try:
                async with session.get(url, **kwargs) as resp:
                    if resp.status != 200:
                        return {}
                    body = await resp.json(content_type=None)
            except Exception:
                return {}

        data = body.get("data") or {}
        attrs = data.get("attributes") or {}
        prices_obj = attrs.get("token_prices") or {}

        out: dict[tuple[str, str], float] = {}
        for addr, price_str in prices_obj.items():
            try:
                price = float(price_str)
            except (TypeError, ValueError):
                continue
            if price > 0:
                out[(network, norm_addr(addr))] = price
        return out
