"""
Jupiter price source — Solana only.

Solana is the registry's biggest population but DexScreener/GeckoTerminal
barely cover it (~6%). Jupiter aggregates essentially all Solana DEX
liquidity, so for Solana mints it's the authoritative source.

Endpoint (keyless, free, IP-rate-limited → proxy rotation):
    https://lite-api.jup.ag/price/v3?ids=<mint,mint,...>   (up to 100)

Response shape:
    { "<mint>": { "usdPrice": 0.99, "liquidity": 1234.5, ... }, ... }
(liquidity is present for many but not all entries — used when given.)
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from utils import get_logger, get_proxy_manager, get_redis, norm_addr

log = get_logger(__name__)

_URL = "https://lite-api.jup.ag/price/v3?ids={ids}"
_BATCH = 100
_PARALLEL = 200
_CACHE_TTL_SEC = 45


class JupiterClient:
    def __init__(self) -> None:
        self._proxies = get_proxy_manager()
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=300, limit_per_host=200),
                timeout=aiohttp.ClientTimeout(total=12),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def fetch_prices(
        self,
        queries: list[tuple[str, str]],   # [("solana", mint), ...]
    ) -> dict[tuple[str, str], dict]:
        """Returns {("solana", mint_norm): {"price": float, "liq": float|0}}."""
        mints = [norm_addr(a) for c, a in queries if c == "solana"]
        if not mints:
            return {}

        r = await get_redis()
        out: dict[tuple[str, str], dict] = {}

        # ── cache read ───────────────────────────────────────────────────
        async with r.pipeline(transaction=False) as pipe:
            for m in mints:
                pipe.get(f"cc2:jup_cache:{m}")
            cached = await pipe.execute()
        miss: list[str] = []
        for m, v in zip(mints, cached):
            if not v:
                miss.append(m)
                continue
            try:
                price, liq = v.split("|")
                out[("solana", m)] = {"price": float(price),
                                      "liq": float(liq) if liq else 0.0}
            except (TypeError, ValueError):
                miss.append(m)
        if not miss:
            return out

        sem = asyncio.Semaphore(_PARALLEL)
        tasks = [self._batch(sem, miss[i:i + _BATCH])
                 for i in range(0, len(miss), _BATCH)]
        fresh: dict[str, dict] = {}
        for res in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(res, dict):
                fresh.update(res)

        if fresh:
            async with r.pipeline(transaction=False) as pipe:
                for m, info in fresh.items():
                    pipe.setex(f"cc2:jup_cache:{m}", _CACHE_TTL_SEC,
                               f"{info['price']}|{info['liq'] or ''}")
                await pipe.execute()
            for m, info in fresh.items():
                out[("solana", m)] = info
        return out

    async def _batch(self, sem: asyncio.Semaphore,
                     mints: list[str]) -> dict[str, dict]:
        url = _URL.format(ids=",".join(mints))
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

        out: dict[str, dict] = {}
        if not isinstance(body, dict):
            return out
        for mint, info in body.items():
            if not isinstance(info, dict):
                continue
            try:
                price = float(info.get("usdPrice") or 0)
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue
            try:
                liq = float(info.get("liquidity") or 0)
            except (TypeError, ValueError):
                liq = 0.0
            out[norm_addr(mint)] = {"price": price, "liq": liq}
        return out
