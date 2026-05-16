"""
Cross-chain price monitor.

Every interval_sec:
  1. Load all token groups from Redis (cg2:group:*).
  2. DexScreener — per-chain price + liquidity (authoritative; real
     per-chain price, no canonical masking). Pools below
     min_pool_liquidity_usd are skipped.
  3. GeckoTerminal — price-only fallback for (chain, addr) DexScreener
     didn't cover (mostly non-EVM / small chains).
  4. Persist cc2:price:{chain}:{addr} and cc2:liq_usd:{chain}:{addr}.

After write, sets cycle_done_event so the detector can scan immediately.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import aiohttp

from config import get_thresholds
from core.geckoterminal import GeckoTerminalClient
from core.jupiter import JupiterClient
from utils import get_logger, get_proxy_manager, get_redis, norm_addr

log = get_logger(__name__)

# Chain → DexScreener slug.
_DS_CHAIN_SLUG = {
    "ethereum":    "ethereum",     "bsc":        "bsc",
    "polygon":     "polygon",      "arbitrum":   "arbitrum",
    "base":        "base",         "optimism":   "optimism",
    "avalanche":   "avalanche",    "fantom":     "fantom",
    "zksync":      "zksync",       "linea":      "linea",
    "blast":       "blast",        "scroll":     "scroll",
    "mantle":      "mantle",       "berachain":  "berachain",
    "celo":        "celo",         "cronos":     "cronos",
    "moonbeam":    "moonbeam",
    "metis":       "metis",        "harmony":    "harmony",
    "iotex":       "iotex",        "rsk":        "rsk",
    "flare":       "flare",        "manta":      "manta",
    "taiko":       "taiko",        "plume":      "plume",
    "shibarium":   "shibarium",    "zircuit":    "zircuit",
    "opbnb":       "opbnb",        "kava":       "kava",
    "core":        "core",         "kaia":       "kaia",
    "ronin":       "ronin",        "zero":       "zero",
    "abstract":    "abstract",     "sonic":      "sonic",
    "sophon":      "sophon",       "hyperliquid":"hyperliquid",
    "monad":       "monad",        "pulsechain": "pulsechain",
    "telos":       "telos",        "neon":       "neon",
    "oasys":       "oasys",        "bitkub":     "bitkub",
    "okex":        "okxchain",
    "solana":      "solana",       "sui":        "sui",
    "aptos":       "aptos",        "tron":       "tron",
    "near":        "near",         "ton":        "ton",
    "sei":         "sei",          "cardano":    "cardano",
    "filecoin":    "filecoin",     "osmosis":    "osmosis",
    "injective":   "injective",    "xrp":        "xrp",
}

# Chains we never price/compare. gnosis (xdai) bridge wrappers chronically
# show a persistent peg gap vs mainnet that isn't an executable arb.
_EXCLUDED_CHAINS = {"gnosis"}


def _price_key(chain: str, addr: str) -> str:
    return f"cc2:price:{chain}:{norm_addr(addr)}"


def _liq_key(chain: str, addr: str) -> str:
    return f"cc2:liq_usd:{chain}:{norm_addr(addr)}"


class CrossChainMonitor:
    def __init__(self) -> None:
        cfg = get_thresholds()["monitor"]
        self._interval    : int   = cfg["interval_sec"]
        self._batch_size  : int   = cfg["batch_size"]
        self._parallel    : int   = cfg["parallel_requests"]
        self._price_ttl   : int   = cfg["price_ttl_sec"]
        self._min_liq_usd : float = cfg.get("min_pool_liquidity_usd", 500)

        self._proxies = get_proxy_manager()
        self._session : aiohttp.ClientSession | None = None
        self._running = False
        self._gt      = GeckoTerminalClient()
        self._jup     = JupiterClient()
        # Detector waits on this instead of sleeping.
        self.cycle_done_event = asyncio.Event()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=1000, limit_per_host=300, ttl_dns_cache=300,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        await self._gt.close()
        await self._jup.close()

    async def start(self) -> None:
        self._running = True
        log.info("cc_monitor.started", interval_sec=self._interval,
                 parallel=self._parallel, proxies=self._proxies.total)
        while self._running:
            t0 = time.monotonic()
            try:
                stats = await self._run_cycle()
                log.info("cc_monitor.cycle_done",
                         elapsed=round(time.monotonic() - t0, 1), **stats)
                self.cycle_done_event.set()
                self.cycle_done_event.clear()
            except Exception as e:
                log.error("cc_monitor.cycle_error", err=str(e))
            sleep_for = max(0, self._interval - (time.monotonic() - t0))
            await asyncio.sleep(sleep_for)

    async def stop(self) -> None:
        self._running = False

    # ── Core ──────────────────────────────────────────────────────────────

    async def _run_cycle(self) -> dict[str, int]:
        r = await get_redis()

        # ── Load groups ──────────────────────────────────────────────────
        keys: list[str] = []
        async for key in r.scan_iter(match="cg2:group:*", count=1000):
            keys.append(key)

        chain_addrs: dict[str, set[str]] = {}
        groups_count = 0
        CHUNK = 1000
        for i in range(0, len(keys), CHUNK):
            batch = keys[i : i + CHUNK]
            async with r.pipeline(transaction=False) as pipe:
                for k in batch:
                    pipe.hgetall(k)
                results = await pipe.execute()
            for h in results:
                if not h or len(h) < 2:
                    continue
                groups_count += 1
                for chain, addr in h.items():
                    if chain in _EXCLUDED_CHAINS:
                        continue
                    chain_addrs.setdefault(chain, set()).add(norm_addr(addr))

        if not chain_addrs:
            return {"groups": 0, "prices_fetched": 0}

        all_queries: list[tuple[str, str]] = [
            (chain, addr)
            for chain, addrs in chain_addrs.items() for addr in addrs
        ]

        price_map: dict[tuple[str, str], dict] = {}

        # ── DexScreener (authoritative per-chain price + liq) ────────────
        t_ds = time.monotonic()
        ds_res = await self._ds_fetch_all(all_queries)
        for (chain, addr), info in ds_res.items():
            price_map[(chain, addr)] = {**info, "src": "ds"}
        t_ds_el = time.monotonic() - t_ds

        # ── Jupiter (authoritative for Solana — DS/GT barely cover it) ──
        t_jup = time.monotonic()
        jup_count = 0
        sol_q = [q for q in all_queries if q[0] == "solana"]
        if sol_q:
            jup_res = await self._jup.fetch_prices(sol_q)
            for (chain, addr), info in jup_res.items():
                if (chain, addr) not in price_map:
                    price_map[(chain, addr)] = {**info, "src": "jup"}
                    jup_count += 1
        t_jup_el = time.monotonic() - t_jup

        # ── GeckoTerminal (price-only fallback for what DS missed) ───────
        missing = [q for q in all_queries if q not in price_map]
        t_gt = time.monotonic()
        gt_count = 0
        if missing:
            gt_res = await self._gt.fetch_prices(missing)
            for (chain, addr), price in gt_res.items():
                if (chain, addr) not in price_map:
                    price_map[(chain, addr)] = {"price": price, "src": "gt"}
                    gt_count += 1
        t_gt_el = time.monotonic() - t_gt

        # ── Persist ──────────────────────────────────────────────────────
        written_price = written_liq = 0
        async with r.pipeline(transaction=False) as pipe:
            for (chain, addr), info in price_map.items():
                price = info.get("price")
                if not price or price <= 0:
                    continue
                pipe.setex(_price_key(chain, addr), self._price_ttl, str(price))
                written_price += 1
                liq = info.get("liq")
                if liq and liq > 0:
                    pipe.setex(_liq_key(chain, addr),
                               self._price_ttl * 3, str(liq))
                    written_liq += 1
            await pipe.execute()

        log.info("phase_timing",
                 ds_sec=round(t_ds_el, 2), jup_sec=round(t_jup_el, 2),
                 gt_sec=round(t_gt_el, 2), ds_n=len(ds_res),
                 jup_n=jup_count, gt_n=gt_count, missing=len(missing))

        return {
            "groups": groups_count,
            "addresses_queried": len(all_queries),
            "prices_fetched": written_price,
            "liq_written": written_liq,
            "ds": len(ds_res),
            "jup": jup_count,
            "gt": gt_count,
        }

    # ── DexScreener bulk fetch ───────────────────────────────────────────

    async def _ds_fetch_all(
        self, queries: list[tuple[str, str]],
    ) -> dict[tuple[str, str], dict]:
        """DS prices+liq for many (chain, addr) in parallel batches.
        Returns {(chain, addr): {"price": ..., "liq": ...}}."""
        by_chain: dict[str, list[str]] = {}
        for chain, addr in queries:
            if chain in _DS_CHAIN_SLUG:
                by_chain.setdefault(chain, []).append(addr)

        sem = asyncio.Semaphore(self._parallel)
        tasks = []
        for chain, addrs in by_chain.items():
            for i in range(0, len(addrs), self._batch_size):
                batch = addrs[i : i + self._batch_size]
                tasks.append(self._ds_batch(sem, chain, batch))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: dict[tuple[str, str], dict] = {}
        for res in results:
            if isinstance(res, Exception) or not res:
                continue
            out.update(res)
        return out

    async def _ds_batch(
        self,
        sem: asyncio.Semaphore,
        chain: str,
        addresses: list[str],
    ) -> dict[tuple[str, str], dict]:
        slug = _DS_CHAIN_SLUG.get(chain)
        if not slug or not addresses:
            return {}

        url = (
            "https://api.dexscreener.com/latest/dex/tokens/"
            f"{','.join(addresses)}"
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
                    pairs = body.get("pairs") or []
            except Exception:
                return {}

        # For each requested address, pick the highest-liquidity pool on the
        # target chain. Pre-filter by min_liq so ghost pools don't poison cache.
        addr_set = {norm_addr(a) for a in addresses}
        best: dict[str, tuple[float, float]] = {}  # addr -> (liq, price)
        for pair in pairs:
            if (pair.get("chainId") or "").lower() != slug:
                continue
            try:
                base_addr = norm_addr(pair["baseToken"]["address"])
                price = float(pair.get("priceUsd") or 0)
                liq   = float((pair.get("liquidity") or {}).get("usd") or 0)
            except (KeyError, TypeError, ValueError):
                continue
            if price <= 0 or base_addr not in addr_set:
                continue
            if liq < self._min_liq_usd:
                continue
            cur = best.get(base_addr)
            if cur is None or liq > cur[0]:
                best[base_addr] = (liq, price)

        return {
            (chain, addr): {"price": price, "liq": liq}
            for addr, (liq, price) in best.items()
        }


async def get_price(chain: str, addr: str) -> float | None:
    r = await get_redis()
    val = await r.get(_price_key(chain, addr))
    return float(val) if val else None
