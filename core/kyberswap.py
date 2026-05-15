"""
KyberSwap aggregator route quotes — execution-grade price/liquidity proof.

Unlike pool-spot prices (DefiLlama / DexScreener), KyberSwap routes through
the actual best pool combination available right now. The returned
`amountInUsd` is what KyberSwap values the input at, and `amountOutUsd` is
what you would actually receive. Together they prove (a) the token is
tradable, and (b) the relative price between two chains.

We use it ONLY in the verifier for top candidates — too heavy for the bulk
cycle (~1-2s per quote).

Endpoint: https://aggregator-api.kyberswap.com/{chain}/api/v1/routes
  ?tokenIn={addr}&tokenOut={USDC}&amountIn={1e18 raw}

Comparison method (verifier uses this):
  Quote both cheap-chain and expensive-chain sides with the same amountIn.
  KyberSwap's amountInUsd already factors in the per-token USD price.
  Since cross-chain wrapped versions of the same token nearly always share
  decimals, the ratio (exp.amountInUsd / cheap.amountInUsd) is the real
  spread — independent of whatever decimal scaling the token has.
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from utils import get_logger, get_proxy_manager

log = get_logger(__name__)

# Our chain → KyberSwap chain slug
_KS_CHAIN_MAP = {
    "ethereum":   "ethereum",
    "bsc":        "bsc",
    "polygon":    "polygon",
    "arbitrum":   "arbitrum",
    "base":       "base",
    "optimism":   "optimism",
    "avalanche":  "avalanche",
    "fantom":     "fantom",
    "linea":      "linea",
    "scroll":     "scroll",
    "mantle":     "mantle",
    "blast":      "blast",
    "zksync":     "zksync",
    "berachain":  "berachain",
    "sonic":      "sonic",
    "ronin":      "ronin",
}

# Canonical USDC (or chain-native stable) per chain — the swap target.
_USDC_ADDR = {
    "ethereum":  "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "bsc":       "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    "polygon":   "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
    "arbitrum":  "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    "base":      "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    "optimism":  "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
    "avalanche": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
    "fantom":    "0x2f733095b80a04b38b0d10cc884524a3d09b836a",
    "linea":     "0x176211869ca2b568f2a7d4ee941e073a821ee1ff",
    "scroll":    "0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4",
    "mantle":    "0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9",
    "blast":     "0x4300000000000000000000000000000000000003",
    "zksync":    "0x1d17cbcf0d6d143135ae902365d2e5e2a16538d4",
    "berachain": "0x549943e04f40284185054145c6e4e9568c1d3241",
    "sonic":     "0x29219dd400f2bf60e5a23d13be72b486d4038894",
    "ronin":     "0x0b7007c13325c48911f73a2dad5fa5dcbf808adc",
}

DEFAULT_AMOUNT_IN_RAW = 10**18
PARALLEL = 200


class KyberSwapClient:
    def __init__(self) -> None:
        self._proxies = get_proxy_manager()
        self._session: aiohttp.ClientSession | None = None
        self._sem = asyncio.Semaphore(PARALLEL)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=300, limit_per_host=200),
                timeout=aiohttp.ClientTimeout(total=8),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def supports(self, chain: str) -> bool:
        return chain in _KS_CHAIN_MAP and chain in _USDC_ADDR

    async def quote(self, chain: str, addr: str) -> dict | None:
        """Returns {"in_usd": ..., "out_usd": ..., "slippage_pct": ...} or None.

        slippage_pct = (in_usd - out_usd) / in_usd * 100 — if > ~5% the pool
        is too thin for our trade size (1e18 raw); caller can drop the alert.
        """
        ks_chain = _KS_CHAIN_MAP.get(chain)
        usdc = _USDC_ADDR.get(chain)
        if not ks_chain or not usdc:
            return None
        addr_lc = addr.lower()
        if addr_lc == usdc.lower():
            return {"in_usd": 1.0, "out_usd": 1.0, "slippage_pct": 0.0}

        url = (
            f"https://aggregator-api.kyberswap.com/{ks_chain}/api/v1/routes"
            f"?tokenIn={addr}&tokenOut={usdc}&amountIn={DEFAULT_AMOUNT_IN_RAW}"
        )
        async with self._sem:
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

        try:
            summary = ((body.get("data") or {}).get("routeSummary") or {})
            in_usd  = float(summary.get("amountInUsd") or 0)
            out_usd = float(summary.get("amountOutUsd") or 0)
        except (TypeError, ValueError):
            return None

        if in_usd <= 0 or out_usd <= 0:
            return None
        slip = (in_usd - out_usd) / in_usd * 100
        return {"in_usd": in_usd, "out_usd": out_usd, "slippage_pct": slip}
