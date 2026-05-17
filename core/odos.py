"""
ODOS aggregated price — alert-time verification for EVM legs.

The bulk monitor uses single-pool DexScreener prices (cheap, wide) to FIND
candidates. That price can be skewed (one imbalanced pool, volatile quote
token, thin pool). Before alerting, we replace each EVM leg's price with
ODOS's routing-graph USD price — what the token is actually worth across
all DEX liquidity on that chain. If ODOS can't price a token (no route),
the leg is treated as non-tradable and dropped.

Keyless: GET https://api.odos.xyz/pricing/token/{chainId}/{addr}
         -> {"currencyId":"USD","price": <float>}
Per-token (no batching) + rate-limited -> only ever called for the handful
of detector candidates, with proxy rotation and a short Redis cache.
"""
from __future__ import annotations

from typing import Any

import aiohttp

from utils import get_logger, get_proxy_manager, get_redis, norm_addr

log = get_logger(__name__)

# Our internal chain name -> ODOS chainId. Only ODOS-supported chains; any
# other chain keeps its DS/GT/Jupiter price (no ODOS verification).
_ODOS_CHAIN = {
    "ethereum":  1,
    "optimism":  10,
    "bsc":       56,
    "polygon":   137,
    "fantom":    250,
    "zksync":    324,
    "mantle":    5000,
    "base":      8453,
    "arbitrum":  42161,
    "avalanche": 43114,
    "linea":     59144,
    "scroll":    534352,
    "sonic":     146,
}

_CACHE_TTL_SEC = 30


def supported(chain: str) -> bool:
    return chain in _ODOS_CHAIN


class OdosClient:
    def __init__(self) -> None:
        self._proxies = get_proxy_manager()
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=200, limit_per_host=100),
                timeout=aiohttp.ClientTimeout(total=8),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def price(self, chain: str, addr: str) -> float | None:
        """ODOS routing-graph USD price. None = unsupported chain / no route
        / error (caller should treat None as 'not tradable')."""
        cid = _ODOS_CHAIN.get(chain)
        if cid is None:
            return None
        a = norm_addr(addr)
        r = await get_redis()
        ck = f"cc2:odos:{cid}:{a}"
        cached = await r.get(ck)
        if cached is not None:
            try:
                v = float(cached)
            except (TypeError, ValueError):
                return None
            return v if v > 0 else None

        url = f"https://api.odos.xyz/pricing/token/{cid}/{a}"
        proxy = self._proxies.next()
        session = await self._get_session()
        kwargs: dict[str, Any] = {"proxy": proxy} if proxy else {}
        price = 0.0
        try:
            async with session.get(url, **kwargs) as resp:
                if resp.status == 200:
                    body = await resp.json(content_type=None)
                    try:
                        price = float(body.get("price") or 0)
                    except (TypeError, ValueError):
                        price = 0.0
        except Exception:
            price = 0.0

        # Cache result (0 too — avoids re-hammering no-route tokens).
        await r.setex(ck, _CACHE_TTL_SEC, str(price))
        return price if price > 0 else None
