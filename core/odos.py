"""
ODOS executable-tradability check — alert-time veto for EVM legs.

The bulk monitor (DS/GT/Jupiter) FINDS candidates with single-pool prices.
Before alerting, each EVM leg is checked with a real ODOS swap quote of a
fixed USD size (default $100 USDC -> token). If ODOS can't route it, or the
price impact at that size is huge (dead/ghost pool), the leg is NOT
tradable and gets dropped — even when a single pool reports a price.

Example (live): USDC->WETH $100 -> priceImpact 0.05%, keep.
                USDC->SESH $100 -> priceImpact 86%, $100 in -> $14 out, drop.

Keyless: POST https://api.odos.xyz/sor/quote/v2
Per-token (no batching) + rate-limited -> candidates-only, proxy-rotated,
cached. Returns None when it can't check (unsupported chain / no stable
mapped) so we never over-drop on a blind spot.
"""
from __future__ import annotations

from typing import Any

import aiohttp

from utils import get_logger, get_proxy_manager, get_redis, norm_addr

log = get_logger(__name__)

# chain -> ODOS chainId
_ODOS_CHAIN = {
    "ethereum":  1,   "optimism":  10,    "bsc":       56,
    "polygon":   137, "fantom":    250,   "zksync":    324,
    "mantle":    5000,"base":      8453,  "arbitrum":  42161,
    "avalanche": 43114,"linea":    59144, "scroll":    534352,
    "sonic":     146,
}

# chain -> (input stable address, decimals). Used as the $-side of the
# quote. Native USDC where possible (USDT on BSC — 18 decimals).
_STABLE = {
    "ethereum":  ("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6),
    "optimism":  ("0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85", 6),
    "polygon":   ("0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359", 6),
    "arbitrum":  ("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", 6),
    "base":      ("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", 6),
    "avalanche": ("0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E", 6),
    "linea":     ("0x176211869cA2b568f2A7D4EE941E073a821EE1ff", 6),
    "scroll":    ("0x06eFdBFf2a14a7c8E15944D1F4A48F9F95F663A4", 6),
    "zksync":    ("0x1d17CBcF0D6D143135aE902365D2E5e2A16538D4", 6),
    "mantle":    ("0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9", 6),
    "sonic":     ("0x29219dd400f2Bf60E5a23d13Be72B486D4038894", 6),
    "bsc":       ("0x55d398326f99059fF775485246999027B3197955", 18),  # USDT
    "fantom":    ("0x28a92dde19D9989F39A49905d7C9C2FAc7799bDf", 6),
}

_CACHE_TTL_SEC = 30
_BURN = "0x0000000000000000000000000000000000000001"


def supported(chain: str) -> bool:
    return chain in _ODOS_CHAIN and chain in _STABLE


class OdosClient:
    def __init__(self) -> None:
        self._proxies = get_proxy_manager()
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=200, limit_per_host=100),
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def price(self, chain: str, addr: str) -> float | None:
        """ODOS routing-graph USD price (GET /pricing). Aggregated across
        all DEX routes -> kills single-pool / volatile-quote price artifacts
        (GEKKO/VIRTUAL, Fabwelt, Maga). None = unsupported / no price."""
        cid = _ODOS_CHAIN.get(chain)
        if cid is None:
            return None
        a = norm_addr(addr)
        r = await get_redis()
        ck = f"cc2:odosp:{cid}:{a}"
        cached = await r.get(ck)
        if cached is not None:
            try:
                v = float(cached)
            except (TypeError, ValueError):
                return None
            return v if v > 0 else None

        url = f"https://api.odos.xyz/pricing/token/{cid}/{a}"
        price = 0.0
        try:
            session = await self._get_session()
            proxy = self._proxies.next()
            kwargs: dict[str, Any] = {"proxy": proxy} if proxy else {}
            async with session.get(url, **kwargs) as resp:
                if resp.status == 200:
                    body = await resp.json(content_type=None)
                    try:
                        price = float(body.get("price") or 0)
                    except (TypeError, ValueError):
                        price = 0.0
        except Exception:
            price = 0.0
        if price > 0:
            await r.setex(ck, _CACHE_TTL_SEC, str(price))
        return price if price > 0 else None

    async def tradable(
        self, chain: str, addr: str,
        usd: float = 100.0, max_impact_pct: float = 5.0,
    ) -> bool | None:
        """True  = $usd swap routes with acceptable price impact.
        False = no route / impact too high (dead/ghost — drop the leg).
        None  = can't check (unsupported chain / no stable) — don't veto.
        """
        cid = _ODOS_CHAIN.get(chain)
        stable = _STABLE.get(chain)
        if cid is None or stable is None:
            return None
        a = norm_addr(addr)
        r = await get_redis()
        ck = f"cc2:odosq:{cid}:{a}:{int(usd)}"
        cached = await r.get(ck)
        if cached is not None:
            return cached == "1"

        stable_addr, dec = stable
        body = {
            "chainId": cid,
            "inputTokens": [{"tokenAddress": stable_addr,
                             "amount": str(int(usd * (10 ** dec)))}],
            "outputTokens": [{"tokenAddress": a, "proportion": 1}],
            "slippageLimitPercent": 0.5,
            "userAddr": _BURN,
            "compact": True,
        }
        ok: bool | None = None
        try:
            session = await self._get_session()
            proxy = self._proxies.next()
            kwargs: dict[str, Any] = {"proxy": proxy} if proxy else {}
            async with session.post("https://api.odos.xyz/sor/quote/v2",
                                    json=body, **kwargs) as resp:
                if resp.status == 200:
                    j = await resp.json(content_type=None)
                    if not j.get("pathId"):
                        ok = False
                    else:
                        try:
                            impact = abs(float(j.get("priceImpact") or 0))
                        except (TypeError, ValueError):
                            impact = 0.0
                        inv = (j.get("inValues") or [0])[0] or 0
                        outv = (j.get("outValues") or [0])[0] or 0
                        keeps = (outv / inv) if inv else 0
                        ok = (impact <= max_impact_pct
                              and keeps >= (1 - max_impact_pct / 100.0))
                elif resp.status in (400, 422):
                    # ODOS replies 400 "no path" for untradable tokens.
                    ok = False
                # other statuses (429/5xx/proxy) -> None: don't veto blindly
        except Exception:
            ok = None

        if ok is not None:
            await r.setex(ck, _CACHE_TTL_SEC, "1" if ok else "0")
        return ok
