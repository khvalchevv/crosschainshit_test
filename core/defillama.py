"""
DefiLlama price fallback for chains DexScreener doesn't cover
(Hyperliquid, Monad, Oasys, Osmosis, Injective, Sei, and many more).

Endpoint: https://coins.llama.fi/prices/current/{chain}:{addr},...
Free, no key, up to ~100 addresses per call.
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from utils import get_logger, get_proxy_manager

log = get_logger(__name__)

# Our internal chain name → DefiLlama chain slug
_DL_CHAIN_MAP = {
    # Major EVM (mostly same name)
    "ethereum":    "ethereum",
    "bsc":         "bsc",
    "polygon":     "polygon",
    "arbitrum":    "arbitrum",
    "base":        "base",
    "optimism":    "optimism",
    "avalanche":   "avax",
    "fantom":      "fantom",
    "zksync":      "era",        # DefiLlama uses "era" for zkSync Era
    "linea":       "linea",
    "blast":       "blast",
    "scroll":      "scroll",
    "mantle":      "mantle",
    "berachain":   "berachain",
    "celo":        "celo",
    "cronos":      "cronos",
    "moonbeam":    "moonbeam",
    "metis":       "metis",
    "harmony":     "harmony",
    "iotex":       "iotex",
    "rsk":         "rsk",
    "flare":       "flare",
    "manta":       "manta",
    "taiko":       "taiko",
    "plume":       "plume",
    "shibarium":   "shibarium",
    "zircuit":     "zircuit",
    "opbnb":       "op_bnb",
    "kava":        "kava",
    "core":        "core",
    "kaia":        "kaia",
    "ronin":       "ronin",
    "zero":        "zero",
    "abstract":    "abstract",
    "sonic":       "sonic",
    "sophon":      "sophon",
    "hyperliquid": "hyperliquid",
    "monad":       "monad",
    "pulsechain":  "pulsechain",
    "telos":       "telos",
    "neon":        "neon_evm",
    "oasys":       "oasys",
    "bitkub":      "bitkub",
    "okex":        "okexchain",
    # Non-EVM
    "solana":      "solana",
    "sui":         "sui",
    "aptos":       "aptos",
    "tron":        "tron",
    "near":        "near",
    "ton":         "ton",
    "sei":         "sei",
    "cardano":     "cardano",
    "filecoin":    "filecoin",
    "osmosis":     "osmosis",
    "injective":   "injective",
    "xrp":         "xrp",
}

BATCH_SIZE = 100             # DefiLlama URL accepts up to ~100 keys
MIN_CONFIDENCE = 0.5         # DefiLlama's confidence score; reject low-quality quotes
PARALLEL = 700               # use most of 1000-proxy pool


class DefiLlamaClient:
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
        """
        Returns {(chain, addr): price_usd} for all that resolved.
        Only keeps results with confidence >= MIN_CONFIDENCE.
        """
        if not queries:
            return {}

        # Translate to DefiLlama format and drop unsupported
        keys: list[str] = []
        key_to_query: dict[str, tuple[str, str]] = {}
        for chain, addr in queries:
            dl_chain = _DL_CHAIN_MAP.get(chain)
            if not dl_chain:
                continue
            key = f"{dl_chain}:{addr.lower()}"
            keys.append(key)
            key_to_query[key] = (chain, addr.lower())

        if not keys:
            return {}

        # Batch + parallel
        batches = [keys[i : i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]
        sem = asyncio.Semaphore(PARALLEL)

        async def _fetch_one(batch: list[str]) -> dict[str, float]:
            async with sem:
                return await self._fetch_batch(batch)

        all_results: dict[str, float] = {}
        for res in await asyncio.gather(*[_fetch_one(b) for b in batches], return_exceptions=True):
            if isinstance(res, Exception):
                continue
            all_results.update(res)

        # Remap to (chain, addr) tuples
        out: dict[tuple[str, str], float] = {}
        for k, price in all_results.items():
            q = key_to_query.get(k)
            if q:
                out[q] = price
        return out

    async def _fetch_batch(self, batch: list[str]) -> dict[str, float]:
        url = "https://coins.llama.fi/prices/current/" + ",".join(batch)
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

        coins = body.get("coins") or {}
        out: dict[str, float] = {}
        for key, data in coins.items():
            if not isinstance(data, dict):
                continue
            try:
                price = float(data.get("price") or 0)
                conf  = float(data.get("confidence") or 0)
            except (TypeError, ValueError):
                continue
            if price > 0 and conf >= MIN_CONFIDENCE:
                out[key] = price
        return out
