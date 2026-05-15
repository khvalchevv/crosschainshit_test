"""
Pre-alert price verifier (Alchemy-free).

Two responsibilities:
  verify()                       — re-query DS (with delay) for both sides of
                                    a candidate, return fresh prices if the
                                    spread persists. Falls back to DefiLlama
                                    when DS doesn't index a chain.
  liquidity_and_volume_check()   — gate alert on per-side liq + 24h vol
                                    pulled from DS. For chains DS doesn't
                                    index, falls back to whatever liq we have
                                    cached in cc2:liq_usd:* (set by monitor
                                    from OKX/GeckoTerminal/Jupiter).

Optional (set arbitrage.kyberswap_verify_enabled in thresholds.yaml):
  before alerting, ask KyberSwap for an aggregator route quote on each side.
  If either side returns no route, alert is suppressed (dead pool).
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import aiohttp

from config import get_thresholds
from core.defillama import DefiLlamaClient
from core.geckoterminal import GeckoTerminalClient
from core.kyberswap import KyberSwapClient
from utils import get_logger, get_proxy_manager

log = get_logger(__name__)

_CHAIN_SLUG = {
    "ethereum":   "ethereum",     "bsc":        "bsc",
    "polygon":    "polygon",      "arbitrum":   "arbitrum",
    "base":       "base",         "optimism":   "optimism",
    "avalanche":  "avalanche",    "fantom":     "fantom",
    "zksync":     "zksync",       "linea":      "linea",
    "blast":      "blast",        "scroll":     "scroll",
    "mantle":     "mantle",       "berachain":  "berachain",
    "celo":       "celo",         "cronos":     "cronos",
    "moonbeam":   "moonbeam",
    "metis":      "metis",        "harmony":    "harmony",
    "iotex":      "iotex",        "rsk":        "rsk",
    "flare":      "flare",        "manta":      "manta",
    "taiko":      "taiko",        "plume":      "plume",
    "shibarium":  "shibarium",    "zircuit":    "zircuit",
    "opbnb":      "opbnb",        "kava":       "kava",
    "core":       "core",         "kaia":       "kaia",
    "ronin":      "ronin",        "zero":       "zero",
    "abstract":   "abstract",     "sonic":      "sonic",
    "sophon":     "sophon",       "hyperliquid":"hyperliquid",
    "monad":      "monad",        "pulsechain": "pulsechain",
    "telos":      "telos",        "neon":       "neon",
    "oasys":      "oasys",        "bitkub":     "bitkub",
    "solana":     "solana",       "sui":        "sui",
    "aptos":      "aptos",        "tron":       "tron",
    "near":       "near",         "ton":        "ton",
    "sei":        "sei",          "cardano":    "cardano",
    "filecoin":   "filecoin",     "osmosis":    "osmosis",
    "injective":  "injective",    "xrp":        "xrp",
}


async def _noop():
    return None


class PriceVerifier:
    """Re-queries DexScreener with a delay to filter stale data."""

    def __init__(self) -> None:
        cfg = get_thresholds()["monitor"]
        acfg = get_thresholds().get("arbitrage", {})
        self._delay_sec: int   = cfg.get("verify_delay_sec", 1)
        self._min_liq  : float = cfg.get("min_pool_liquidity_usd", 5000)
        self._kyber_verify : bool = acfg.get("kyberswap_verify_enabled", False)
        self._kyber_max_slippage_pct: float = acfg.get(
            "kyberswap_max_slippage_pct", 5.0
        )
        self._proxies = get_proxy_manager()
        self._session: aiohttp.ClientSession | None = None
        self._llama  = DefiLlamaClient()
        self._gt     = GeckoTerminalClient()
        self._kyber  = KyberSwapClient()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=1000),
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        await self._llama.close()
        await self._gt.close()
        await self._kyber.close()

    async def verify(
        self,
        cheap_chain: str,
        cheap_addr: str,
        expensive_chain: str,
        expensive_addr: str,
    ) -> tuple[float, float] | None:
        """Re-fetch fresh prices for both sides after a short delay.

        Strategy:
          1) Sleep verify_delay_sec so we read post-detection price.
          2) Query DS for each side in parallel (chain has DS slug).
          3) For sides DS doesn't cover → fall back to DefiLlama.
          4) If still missing on either side → return None.
        """
        await asyncio.sleep(self._delay_sec)

        async def _ds_side(chain: str, addr: str) -> float | None:
            info = await self._fetch_full_info(chain, addr)
            return info["price"] if info else None

        cheap_price, exp_price = await asyncio.gather(
            _ds_side(cheap_chain, cheap_addr) if _CHAIN_SLUG.get(cheap_chain) else _noop(),
            _ds_side(expensive_chain, expensive_addr) if _CHAIN_SLUG.get(expensive_chain) else _noop(),
        )

        # Fallback to DefiLlama for sides DS missed (different chains can
        # individually fall back without re-querying the other).
        missing: list[tuple[str, str]] = []
        if not cheap_price:
            missing.append((cheap_chain, cheap_addr))
        if not exp_price:
            missing.append((expensive_chain, expensive_addr))
        if missing:
            fresh = await self._llama.fetch_prices(missing)
            if not cheap_price:
                cheap_price = fresh.get((cheap_chain, cheap_addr.lower()))
            if not exp_price:
                exp_price = fresh.get((expensive_chain, expensive_addr.lower()))

        if not cheap_price or not exp_price:
            return None
        if cheap_price <= 0 or exp_price <= 0:
            return None
        return (cheap_price, exp_price)

    async def aggregator_verify(
        self,
        cheap_chain: str, cheap_addr: str,
        exp_chain: str, exp_addr: str,
    ) -> bool:
        """Optional last-mile check: ask KyberSwap for a real route on both
        sides. If neither side is on a KyberSwap-supported chain, trust the
        earlier DS verify (return True). If one side is supported and fails,
        kill the alert (dead pool / non-tradable)."""
        if not self._kyber_verify:
            return True

        cheap_supported = self._kyber.supports(cheap_chain)
        exp_supported   = self._kyber.supports(exp_chain)
        if not cheap_supported and not exp_supported:
            return True

        async def _q(chain: str, addr: str) -> dict | None:
            if not self._kyber.supports(chain):
                return {"in_usd": 1.0, "out_usd": 1.0, "slippage_pct": 0.0}
            return await self._kyber.quote(chain, addr)

        cheap_q, exp_q = await asyncio.gather(
            _q(cheap_chain, cheap_addr),
            _q(exp_chain, exp_addr),
        )
        if cheap_q is None or exp_q is None:
            log.info("verifier.kyber_no_route",
                     cheap=f"{cheap_chain}:{cheap_addr[:10]}",
                     exp=f"{exp_chain}:{exp_addr[:10]}")
            return False
        if cheap_q["slippage_pct"] > self._kyber_max_slippage_pct:
            log.info("verifier.kyber_high_slip",
                     side="cheap", slip=round(cheap_q["slippage_pct"], 2))
            return False
        if exp_q["slippage_pct"] > self._kyber_max_slippage_pct:
            log.info("verifier.kyber_high_slip",
                     side="exp", slip=round(exp_q["slippage_pct"], 2))
            return False
        return True

    async def _fetch_full_info(self, chain: str, addr: str) -> dict | None:
        """Return {price, liquidity, vol_h24, pair_created_at} for best DS pool."""
        slug = _CHAIN_SLUG.get(chain)
        if not slug:
            return None
        url = f"https://api.dexscreener.com/latest/dex/tokens/{addr}"
        proxy = self._proxies.next()
        session = await self._get_session()
        kwargs: dict[str, Any] = {"proxy": proxy} if proxy else {}
        try:
            async with session.get(url, **kwargs) as resp:
                if resp.status != 200:
                    return None
                body = await resp.json(content_type=None)
                pairs = body.get("pairs") or []
        except Exception:
            return None

        best: dict | None = None
        target = addr.lower()
        for pair in pairs:
            if (pair.get("chainId") or "").lower() != slug:
                continue
            try:
                base_addr  = pair["baseToken"]["address"].lower()
                quote_addr = pair["quoteToken"]["address"].lower()
                price = float(pair.get("priceUsd") or 0)
                liq   = float((pair.get("liquidity") or {}).get("usd") or 0)
                vol24 = float((pair.get("volume") or {}).get("h24") or 0)
            except (KeyError, TypeError, ValueError):
                continue
            if base_addr != target and quote_addr != target:
                continue
            if price <= 0:
                continue
            pair_created_at = pair.get("pairCreatedAt") or 0
            if best is None or liq > best["liquidity"]:
                best = {"price": price, "liquidity": liq, "vol_h24": vol24,
                        "pair_created_at": pair_created_at}
        return best

    async def liquidity_and_volume_check(
        self,
        cheap_chain: str, cheap_addr: str,
        exp_chain: str, exp_addr: str,
        min_liq_usd: float,
        min_vol_usd: float = 0.0,
        new_pool_max_age_hours: float = 24.0,
        new_pool_min_vol_usd: float = 500.0,
    ) -> tuple[bool, dict | None, dict | None]:
        """Returns (ok, cheap_info, exp_info).

        Per-side gate logic (no on-chain reads anymore):
          1) DS slug exists → require DS data AND liq+vol ≥ thresholds.
          2) DS slug missing → check cc2:liq_usd:* cached from monitor's
             OKX/GT/Jupiter pass. If present and ≥ threshold → ok. Vol skipped
             (we don't have non-DS volume data for non-EVM small chains).
        """
        from utils import get_redis
        r = await get_redis()

        cheap_slug = _CHAIN_SLUG.get(cheap_chain)
        exp_slug   = _CHAIN_SLUG.get(exp_chain)

        # Read cached liq from monitor (OKX/GT/Jupiter wrote some of these).
        async with r.pipeline(transaction=False) as pipe:
            pipe.get(f"cc2:liq_usd:{cheap_chain}:{cheap_addr.lower()}")
            pipe.get(f"cc2:liq_usd:{exp_chain}:{exp_addr.lower()}")
            cached = await pipe.execute()

        def _f(v) -> float:
            try: return float(v) if v is not None else 0.0
            except (TypeError, ValueError): return 0.0
        cached_cheap_liq = _f(cached[0])
        cached_exp_liq   = _f(cached[1])

        # Fetch DS for both sides in parallel — needed for fresh price+liq+vol.
        results = await asyncio.gather(
            self._fetch_full_info(cheap_chain, cheap_addr) if cheap_slug else _noop(),
            self._fetch_full_info(exp_chain, exp_addr) if exp_slug else _noop(),
        )
        cheap_info, exp_info = results

        # Build merged info that prefers DS data when present, else cached liq.
        def _merge(cached_liq: float, ds_info: dict | None) -> dict | None:
            if ds_info is not None:
                return {
                    "liquidity": max(cached_liq, ds_info.get("liquidity", 0)),
                    "vol_h24":   ds_info.get("vol_h24", 0.0),
                    "pair_created_at": ds_info.get("pair_created_at", 0),
                }
            if cached_liq > 0:
                return {"liquidity": cached_liq, "vol_h24": 0.0,
                        "pair_created_at": 0}
            return None

        merged_cheap = _merge(cached_cheap_liq, cheap_info)
        merged_exp   = _merge(cached_exp_liq, exp_info)

        def _side_ok(cached_liq: float, ds_info: dict | None,
                     slug: str | None) -> bool:
            ds_liq = (ds_info or {}).get("liquidity", 0)
            if slug and ds_info is not None:
                effective_liq = max(cached_liq, ds_liq)
            elif cached_liq > 0:
                effective_liq = cached_liq
            elif not slug:
                # No DS coverage AND no cached liq from OKX/GT — accept blind.
                # Volume gate below will drop genuinely dead pools when DS does
                # cover the other side; if neither side has DS, we trust the
                # spread on faith (small/exotic chains rarely have wash-trade
                # cross-chain spreads worth alerting if pool isn't real).
                effective_liq = float("inf")
            else:
                effective_liq = 0

            if effective_liq < min_liq_usd:
                return False

            # Volume floor — only enforced when DS provides vol data.
            if slug and ds_info is not None:
                vol = ds_info.get("vol_h24") or 0
                if vol < 500:
                    return False
                created_ms = ds_info.get("pair_created_at") or 0
                age_h = ((time.time() * 1000) - created_ms) / 3_600_000 \
                    if created_ms else 999
                effective_min_vol = (
                    new_pool_min_vol_usd if age_h < new_pool_max_age_hours
                    else min_vol_usd
                )
                if vol < effective_min_vol:
                    return False
            return True

        if not _side_ok(cached_cheap_liq, cheap_info, cheap_slug):
            return (False, merged_cheap, merged_exp)
        if not _side_ok(cached_exp_liq, exp_info, exp_slug):
            return (False, merged_cheap, merged_exp)

        return (True, merged_cheap, merged_exp)

    # back-compat
    async def liquidity_check(self, *args, **kwargs) -> bool:
        ok, _, _ = await self.liquidity_and_volume_check(*args, **kwargs)
        return ok
