"""
Cross-chain price monitor (Alchemy-free).

Pipeline runs every interval_sec:
  Phase A — DexScreener + GeckoTerminal (per-chain, batched, authoritative)
            for everything in their known-sets. These give real per-chain
            prices (no canonical masking).
  Phase B — DefiLlama batch as last-resort fallback ONLY for tokens no
            per-chain source covered (Llama returns origin-chain canonical
            prices for bridged tokens, so it's never preferred).
  Phase C — Persist: writes cc2:price:{chain}:{addr} (price) and
            cc2:liq_usd:{chain}:{addr} (when source provides liquidity).
  (OKX removed — it hard-403-blocked our proxy IPs and yielded ~0.)

After write, sets cycle_done_event so detector can run a scan immediately.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import aiohttp

from config import get_thresholds
from core.defillama import DefiLlamaClient
from core.geckoterminal import GeckoTerminalClient
from utils import get_logger, get_proxy_manager, get_redis

log = get_logger(__name__)

# Chain → DexScreener slug (DS uses chainId in pair objects, this is the
# canonical mapping)
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


def _price_key(chain: str, addr: str) -> str:
    return f"cc2:price:{chain}:{addr.lower()}"


# Chains we never price/compare. gnosis (xdai) bridge wrappers chronically
# show a persistent ~5-8% peg gap vs mainnet that isn't a real executable
# arb (it's bridge friction), creating constant noise.
_EXCLUDED_CHAINS = {"gnosis"}


def _liq_key(chain: str, addr: str) -> str:
    return f"cc2:liq_usd:{chain}:{addr.lower()}"


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
        self._llama   = DefiLlamaClient()
        self._gt      = GeckoTerminalClient()
        # Set after each cycle; detector waits on this instead of sleeping.
        self.cycle_done_event = asyncio.Event()
        # Source provenance — tokens we know each source can price.
        # Built up over time; queried in-memory each cycle for fast filter.
        # Periodically reset to handle delisted tokens.
        self._cycle_count = 0
        self._last_discovery_at = 0.0  # monotonic; gates discovery rotation
        # On startup, do ONE full-discovery pass that sends every unknown
        # token to every per-chain source. Otherwise we'd take ~4h of
        # 30-min discovery rotation to find all DS-eligible tokens —
        # 42% of "Llama-only" coverage is actually DS-eligible.
        self._startup_full_pass_pending = True
        self._known_ds : set[str] = set()
        self._known_gt : set[str] = set()

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
        await self._llama.close()
        await self._gt.close()

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

        async def _timed(name: str, coro):
            t = time.monotonic()
            try:
                res = await coro
            except Exception as e:
                log.warning(f"phase.{name}_err", err=str(e)[:80])
                return name, time.monotonic() - t, e
            return name, time.monotonic() - t, res

        # ── PHASE 0: Load groups from Redis ──────────────────────────────
        t_scan = time.monotonic()
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
                    chain_addrs.setdefault(chain, set()).add(addr.lower())
        t_scan_el = time.monotonic() - t_scan

        if not chain_addrs:
            return {"groups": 0, "prices_fetched": 0}

        all_queries: list[tuple[str, str]] = [
            (chain, addr) for chain, addrs in chain_addrs.items() for addr in addrs
        ]

        # ── PHASE A: DefiLlama batch ─────────────────────────────────────
        # Llama returns canonical/origin-chain prices for bridged tokens
        # (e.g. wrapped OHM on Arbitrum returns mainnet OHM price). That
        # masks real per-chain spreads. So we use per-chain sources
        # (DS/OKX/GT) as authoritative and fall back to Llama only when
        # no per-chain source has data.
        _, t_llama, llama_res = await _timed(
            "llama", self._llama.fetch_prices(all_queries)
        )
        if isinstance(llama_res, Exception):
            llama_res = {}

        price_map: dict[tuple[str, str], dict] = {}

        # ── PHASE B: gap fill — filtered per-source provenance ───────────
        # Each source (DS/GT/OKX) only ever returns prices for tokens it
        # actually indexes. Sending all 13k missing → only ~500 hits per
        # source = 95% wasted requests. Instead, after each successful
        # fetch we remember (chain, addr) in a per-source set; next cycle
        # we send only known tokens. Discovery: every 10th cycle, also
        # include 500 random unknowns to find newly-indexed coverage.
        await self._refresh_known_sets()
        missing: list[tuple[str, str]] = [
            q for q in all_queries if q not in price_map
        ]
        ds_count = gt_count = 0
        t_ds = t_gt = 0.0
        ds_target_n = gt_target_n = 0

        if missing:
            # Startup: ONE full-discovery pass sends every unknown to every
            # source so the known sets become accurate from cycle 2 onwards.
            # Steady-state: discovery rotation every 10 min, 3000 unknowns
            # per source, so newly-indexed tokens get picked up within ~30
            # min instead of ~4 hours.
            now = time.monotonic()
            do_discovery = (now - self._last_discovery_at) >= 10 * 60
            if do_discovery:
                self._last_discovery_at = now
            if self._startup_full_pass_pending:
                ds_targets = gt_targets = missing
                self._startup_full_pass_pending = False
                log.info("monitor.startup_full_discovery", n=len(missing))
            else:
                ds_targets = self._filter_for_source(missing, self._known_ds, do_discovery)
                gt_targets = self._filter_for_source(missing, self._known_gt, do_discovery)
            ds_target_n, gt_target_n = len(ds_targets), len(gt_targets)

            timed_tasks = [
                _timed("ds", self._ds_fetch_all(ds_targets)),
                _timed("gt", self._gt.fetch_prices(gt_targets)),
            ]
            phase_b = await asyncio.gather(*timed_tasks, return_exceptions=True)

            results_by_name: dict[str, tuple[float, object]] = {}
            for entry in phase_b:
                if isinstance(entry, Exception):
                    continue
                name, elapsed, res = entry
                results_by_name[name] = (elapsed, res)

            t_ds, ds_res = results_by_name.get("ds", (0.0, {}))
            t_gt, gt_res = results_by_name.get("gt", (0.0, {}))

            if isinstance(ds_res, Exception): ds_res = {}
            if isinstance(gt_res, Exception): gt_res = {}

            # Per-chain sources fill price_map first — they're authoritative.
            for (chain, addr), info in ds_res.items():
                price_map[(chain, addr)] = {**info, "src": "ds"}
                ds_count += 1
            for (chain, addr), price in gt_res.items():
                if (chain, addr) not in price_map:
                    price_map[(chain, addr)] = {"price": price, "src": "gt"}
                    gt_count += 1

            # Update per-source known sets (in-memory + Redis)
            await self._record_known("ds", ds_res.keys())
            await self._record_known("gt", gt_res.keys())

        # Llama as last-resort fallback for everything per-chain didn't cover.
        llama_count = 0
        for (chain, addr), price in llama_res.items():
            if (chain, addr) not in price_map:
                price_map[(chain, addr)] = {"price": price, "src": "llama"}
                llama_count += 1

        # ── PHASE C: Persist (pipelined) ─────────────────────────────────
        t_persist = time.monotonic()
        written_price = 0
        written_liq = 0
        async with r.pipeline(transaction=False) as pipe:
            for (chain, addr), info in price_map.items():
                price = info.get("price")
                if not price or price <= 0:
                    continue
                pipe.setex(_price_key(chain, addr), self._price_ttl, str(price))
                written_price += 1
                liq = info.get("liq")
                if liq and liq > 0:
                    pipe.setex(_liq_key(chain, addr), self._price_ttl * 3, str(liq))
                    written_liq += 1
            await pipe.execute()
        t_persist_el = time.monotonic() - t_persist

        log.info("phase_timing",
                 scan_sec=round(t_scan_el, 2),
                 llama_sec=round(t_llama, 2),
                 ds_sec=round(t_ds, 2),
                 gt_sec=round(t_gt, 2),
                 persist_sec=round(t_persist_el, 2),
                 missing=len(missing),
                 ds_n=ds_target_n, gt_n=gt_target_n,
                 known_ds=len(self._known_ds),
                 known_gt=len(self._known_gt))

        self._cycle_count += 1

        return {
            "groups": groups_count,
            "addresses_queried": len(all_queries),
            "prices_fetched": written_price,
            "liq_written": written_liq,
            "llama": llama_count,
            "llama_total": len(llama_res),
            "ds": ds_count,
            "gt": gt_count,
        }

    # ── Source provenance cache ──────────────────────────────────────────

    # How often to reset known sets (forces full re-discovery, drops delisted).
    # 24h is conservative — discovery rotation (every 30 min) handles new
    # tokens; reset only needed to evict tokens that became unindexed.
    _KNOWN_RESET_SEC = 24 * 3600

    # Random unknown-token discovery quota per discovery cycle (every 10 min).
    _DISCOVERY_QUOTA = 3000

    async def _refresh_known_sets(self) -> None:
        """Pull per-source known token sets from Redis at cycle start.
        Auto-resets every _KNOWN_RESET_SEC seconds to handle delisting."""
        now = time.monotonic()
        if not hasattr(self, "_last_reset_at"):
            self._last_reset_at = now  # first-ever call: anchor, don't reset
        elif now - self._last_reset_at >= self._KNOWN_RESET_SEC:
            r = await get_redis()
            await r.delete("cc2:src_known:ds", "cc2:src_known:gt")
            log.info("monitor.known_sets_reset")
            self._known_ds.clear()
            self._known_gt.clear()
            self._last_reset_at = now
            return

        r = await get_redis()
        async with r.pipeline(transaction=False) as pipe:
            pipe.smembers("cc2:src_known:ds")
            pipe.smembers("cc2:src_known:gt")
            ds_m, gt_m = await pipe.execute()
        self._known_ds = set(ds_m or [])
        self._known_gt = set(gt_m or [])

    def _filter_for_source(
        self,
        missing: list[tuple[str, str]],
        known: set[str],
        do_discovery: bool,
    ) -> list[tuple[str, str]]:
        """Return only (chain, addr) that the source previously priced.
        On discovery cycles, also include a random sample of unknown tokens.
        Cold start (empty known set): pass everything through (bootstrap)."""
        if not known:
            return missing  # bootstrap: nothing known yet, send all
        known_targets = [(c, a) for c, a in missing if f"{c}:{a}" in known]
        if not do_discovery:
            return known_targets
        unknown = [(c, a) for c, a in missing if f"{c}:{a}" not in known]
        if not unknown:
            return known_targets
        import random
        sample = random.sample(unknown, min(self._DISCOVERY_QUOTA, len(unknown)))
        return known_targets + sample

    async def _record_known(
        self,
        src: str,
        keys: "Iterable[tuple[str, str]]",
    ) -> None:
        """Persist new (chain, addr) pairs the source successfully priced."""
        new_members = [f"{c}:{a}" for c, a in keys]
        if not new_members:
            return
        r = await get_redis()
        await r.sadd(f"cc2:src_known:{src}", *new_members)
        # Update in-memory mirror so subsequent cycle hits don't need refresh
        if src == "ds":
            self._known_ds.update(new_members)
        elif src == "gt":
            self._known_gt.update(new_members)

    # ── DexScreener bulk fetch (used only inside _run_cycle) ──────────────

    async def _ds_fetch_all(
        self, queries: list[tuple[str, str]],
    ) -> dict[tuple[str, str], dict]:
        """Fetches DS prices+liq for many (chain, addr) in parallel batches.
        Returns {(chain, addr): {"price": ..., "liq": ...}}."""
        # Group by chain so a single DS batch URL contains addrs of one chain
        # (DS pair list returns mixed chains otherwise — slug check filters).
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

        url = f"https://api.dexscreener.com/latest/dex/tokens/{','.join(addresses)}"
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
        # target chain. Pre-filter by min_liq so we don't poison cache with
        # ghost-pool prices.
        addr_set = {a.lower() for a in addresses}
        best: dict[str, tuple[float, float]] = {}  # addr -> (liq, price)
        for pair in pairs:
            if (pair.get("chainId") or "").lower() != slug:
                continue
            try:
                base_addr = pair["baseToken"]["address"].lower()
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
