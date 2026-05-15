"""
Iterates all cg2:group:* and compares cached prices across chains.
Fires callback when net spread > threshold.
"""
from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

from config import get_thresholds
from core.bridge_costs import estimate_bridge_cost, estimate_bridge_cost_pct
from core.monitor import _EXCLUDED_CHAINS, get_price
from core.verifier import PriceVerifier
from utils import get_logger, get_redis

log = get_logger(__name__)


@dataclass
class CrossChainOpportunity:
    cg_id: str
    symbol: str
    cheap_chain: str
    cheap_addr: str
    cheap_price: float
    expensive_chain: str
    expensive_addr: str
    expensive_price: float
    gross_spread_pct: float
    bridge_cost_usd: float
    bridge_cost_pct: float
    net_profit_pct: float
    net_profit_usd: float
    trade_size_usd: float
    # Pool-side metrics (may be None if DS doesn't index that chain)
    cheap_liq_usd: float | None = None
    cheap_vol_h24_usd: float | None = None
    expensive_liq_usd: float | None = None
    expensive_vol_h24_usd: float | None = None
    detected_at: float = field(default_factory=time.time)

    def summary(self) -> str:
        return (
            f"{self.symbol} | BUY {self.cheap_chain} @ ${self.cheap_price:.6g} "
            f"-> SELL {self.expensive_chain} @ ${self.expensive_price:.6g} | "
            f"gross={self.gross_spread_pct:.2f}% "
            f"net={self.net_profit_pct:.2f}% (${self.net_profit_usd:.1f})"
        )


CrossChainCallback = Callable[[CrossChainOpportunity], Coroutine[Any, Any, None]]


class CrossChainDetector:
    def __init__(
        self,
        on_opportunity: CrossChainCallback,
        cycle_trigger: asyncio.Event | None = None,
        update_queue: asyncio.Queue | None = None,
    ) -> None:
        self._on_opportunity = on_opportunity
        self._running = False
        self._trigger = cycle_trigger
        # Event-driven path: WS swap_listener pushes (chain, addr) here on
        # every fresh price write. Workers consume and run a per-group check
        # within ~200ms, so alert latency drops from cycle (3-15s) to ~1-2s.
        self._update_queue = update_queue

        cfg = get_thresholds()["arbitrage"]
        self._min_profit     : float = cfg["min_profit_percent"]
        self._max_profit     : float = cfg["max_profit_percent"]
        self._trade_size_usd : float = cfg["trade_size_usd"]
        self._cooldown_sec   : int   = cfg["alert_cooldown_sec"]
        self._max_age_sec    : int   = cfg.get("max_spread_age_sec", 7200)
        self._warmup_sec     : int   = cfg.get("warmup_minutes", 15) * 60
        self._alert_min_liq  : float = get_thresholds()["monitor"].get(
            "alert_min_pool_liquidity_usd", 10000)
        self._alert_min_vol  : float = get_thresholds()["monitor"].get(
            "alert_min_pool_volume_usd", 1000)
        self._new_pool_max_age_h : float = get_thresholds()["monitor"].get(
            "new_pool_max_age_hours", 24)
        self._new_pool_min_vol   : float = get_thresholds()["monitor"].get(
            "new_pool_min_volume_usd", 500)
        self._started_at: float | None = None
        # cg_ids we've already evaluated with >=2 prices THIS run. Cleared
        # on start so every token's spread-at-first-sighting is treated as
        # "pre-existing" (junk nobody equalized) and parked for 24h — this
        # is the warmup behaviour, but applied per-token at first sighting
        # rather than only at the global startup snapshot. Closes the hole
        # where a token discovered hours into a run fires its stale,
        # year-old spread as if it were a fresh opportunity.
        self._seen_cg: set[str] = set()

        self._interval_sec = get_thresholds()["monitor"]["interval_sec"]
        self._verifier = PriceVerifier()

    async def close(self) -> None:
        await self._verifier.close()

    async def start(self) -> None:
        self._running = True
        self._started_at = time.time()
        log.info("cc_detector.started",
                 min_profit_pct=self._min_profit,
                 warmup_sec=self._warmup_sec)
        # Background: build + maintain structural-spread blacklist
        asyncio.create_task(self._recheck_structural_loop(),
                            name="cc_detector.structural")
        # Event-driven workers: consume (chain, addr) updates from WS,
        # resolve to cg_id and run a single-group check immediately.
        if self._update_queue is not None:
            for i in range(8):
                asyncio.create_task(self._event_worker(i),
                                    name=f"cc_detector.event_worker_{i}")
        warmup_announced = False
        while self._running:
            t0 = time.monotonic()
            in_warmup = self._in_warmup()
            if not in_warmup and not warmup_announced:
                log.info("cc_detector.warmup_complete")
                warmup_announced = True
            try:
                result = await self._scan()
                log.info("cc_detector.cycle_done",
                         elapsed=round(time.monotonic() - t0, 1),
                         candidates=result["candidates"],
                         opportunities=result["alerted"],
                         warmup=in_warmup)
            except Exception as e:
                log.error("cc_detector.error", err=str(e))
            sleep_for = max(0, 60 - (time.monotonic() - t0))
            await asyncio.sleep(sleep_for)

    def _in_warmup(self) -> bool:
        if self._started_at is None:
            return True
        return (time.time() - self._started_at) < self._warmup_sec

    def _dynamic_min_profit(self, smaller_liq_usd: float) -> float:
        """Required spread % to alert. Floor = config min_profit_percent.
        On top, demand 2× expected slippage. Bridge cost is intentionally NOT
        deducted — real bridge cost is non-trivial (depends on route, asset,
        timing) and current flat estimate misleads more than it helps.

        If liq is unknown (warmup not finished / non-EVM chain / cold pool),
        DO NOT inflate the threshold — fall back to the floor. The downstream
        liq+vol gate (DS) will catch genuinely thin pools."""
        if smaller_liq_usd <= 0:
            return self._min_profit
        slip_per_side = (self._trade_size_usd / smaller_liq_usd) * 100
        return max(self._min_profit, 2 * slip_per_side + 1.0)

    @staticmethod
    def _is_stable_or_bluechip(cg_id: str) -> bool:
        """Tokens where pools must be deep (any 'spread' on a thin pool is
        almost certainly a mispriced ghost pool — real stables don't move 5%
        on a $5k pool). Memecoins and longtail tokens get a lower threshold."""
        name = cg_id.lower()
        # Stables
        if any(t in name for t in ("usd", "usdt", "usdc", "dai", "frax",
                                    "tether", "stable", "lusd", "susd",
                                    "tusd", "gusd", "pyusd", "fdusd")):
            return True
        # Wrapped majors
        if name in ("weth", "eth", "wbtc", "btc", "wbnb", "bnb", "wmatic",
                    "matic", "pol", "wpol", "wavax", "avax", "wsol", "sol",
                    "tbtc", "cbbtc", "lbtc", "renbtc"):
            return True
        return False

    async def stop(self) -> None:
        self._running = False

    # ── Structural-spread blacklist ───────────────────────────────────────
    # Tokens with persistent huge spreads (dead bridge, broken pool, mismap)
    # would otherwise fire alerts every 24h cooldown forever. We snapshot
    # those at startup and re-evaluate periodically.

    # NOTE: this is NOT a magnitude legitimacy filter — a freshly-detected
    # 1000% spread can be a real arb. This threshold only decides which
    # pre-existing spreads get parked in the startup dedup so the bot
    # doesn't re-spam chronic dead-bridge/broken-pool spreads on every
    # run. Persistence (24h recheck) + verifier/liq/kyber decide fakeness.
    STRUCTURAL_THRESHOLD = 20.0  # % — pre-existing spread parked at startup
    STRUCTURAL_RECHECK_SEC = 86400  # 24h — re-evaluate parked junk once a day

    async def _structural_blacklist(self) -> set[str]:
        r = await get_redis()
        return {
            (m.decode() if isinstance(m, bytes) else m)
            for m in (await r.smembers("cc2_structural") or set())
        }

    async def _populate_structural(self) -> int:
        """Scan once: every group with spread >= STRUCTURAL_THRESHOLD goes to
        the blacklist. Called once after initial warmup, then every 24h."""
        r = await get_redis()
        added = 0
        async for key in r.scan_iter(match="cg2:group:*", count=1000):
            ks = key.decode() if isinstance(key, bytes) else key
            cg_id = ks.split(":", 2)[-1]
            h = await r.hgetall(key)
            if not h or len(h) < 2:
                continue
            prices: list[float] = []
            for chain, addr in h.items():
                if chain in _EXCLUDED_CHAINS:
                    continue
                p = await get_price(chain, addr.lower())
                if p and p > 0:
                    prices.append(p)
            if len(prices) < 2:
                continue
            cheap = min(prices)
            exp = max(prices)
            if cheap <= 0:
                continue
            spread = (exp - cheap) / cheap * 100
            if spread >= self.STRUCTURAL_THRESHOLD:
                await r.sadd("cc2_structural", cg_id)
                added += 1
        return added

    async def _recheck_structural_loop(self) -> None:
        """Run once at startup (after first prices arrive), then every 24h.
        Re-test each blacklist entry: if spread dropped below threshold, drop
        it from blacklist so it can alert again."""
        # Wait for monitor to populate prices once + WS to push some
        await asyncio.sleep(90)
        # Initial population
        n = await self._populate_structural()
        log.info("cc_detector.structural.populated", count=n)
        while self._running:
            await asyncio.sleep(self.STRUCTURAL_RECHECK_SEC)
            r = await get_redis()
            removed = 0
            members = await r.smembers("cc2_structural") or set()
            for m in members:
                cg_id = m.decode() if isinstance(m, bytes) else m
                h = await r.hgetall(f"cg2:group:{cg_id}")
                if not h or len(h) < 2:
                    await r.srem("cc2_structural", cg_id)
                    removed += 1
                    continue
                prices: list[float] = []
                for chain, addr in h.items():
                    if chain in _EXCLUDED_CHAINS:
                        continue
                    p = await get_price(chain, addr.lower())
                    if p and p > 0:
                        prices.append(p)
                if len(prices) < 2:
                    continue
                cheap, exp = min(prices), max(prices)
                spread = (exp - cheap) / cheap * 100
                if spread < self.STRUCTURAL_THRESHOLD:
                    await r.srem("cc2_structural", cg_id)
                    removed += 1
            # Add any new structural offenders
            added = await self._populate_structural()
            log.info("cc_detector.structural.recheck",
                     removed=removed, added=added)

    async def _event_worker(self, worker_id: int) -> None:
        """Consume (chain, addr) updates from WS and run single-group checks
        instantly. Each worker is independent, so we can have N parallel."""
        r = await get_redis()
        while self._running:
            try:
                chain, addr = await self._update_queue.get()
            except asyncio.CancelledError:
                raise
            try:
                # Skip while in initial warmup so we don't burst on cached spreads
                if self._in_warmup():
                    continue
                # Resolve token → cg_id (the group that owns this address)
                cg_id_raw = await r.get(f"cg2:contract:{chain}:{addr.lower()}")
                if not cg_id_raw:
                    continue
                cg_id = (cg_id_raw.decode()
                         if isinstance(cg_id_raw, bytes) else cg_id_raw)
                # Cheap pre-checks before fetching the full group
                if cg_id in await self._structural_blacklist():
                    continue
                if await r.sismember("cc2_blacklist", cg_id.lower()):
                    continue
                group = await r.hgetall(f"cg2:group:{cg_id}")
                if not group or len(group) < 2:
                    continue
                await self._check_group(cg_id, group)
            except Exception as e:
                log.debug("cc_detector.event_worker_err",
                          worker=worker_id, err=str(e)[:80])

    async def _scan(self) -> dict[str, int]:
        r = await get_redis()
        structural = await self._structural_blacklist()

        # ── Phase 1a: pipeline-fetch ALL groups and blacklist in bulk ────
        group_keys: list[str] = []
        async for key in r.scan_iter(match="cg2:group:*", count=1000):
            group_keys.append(key)

        if not group_keys:
            return {"candidates": 0, "alerted": 0}

        # Bulk HGETALL via pipeline
        groups: dict[str, dict[str, str]] = {}
        CHUNK = 1000
        for i in range(0, len(group_keys), CHUNK):
            batch = group_keys[i : i + CHUNK]
            async with r.pipeline(transaction=False) as pipe:
                for k in batch:
                    pipe.hgetall(k)
                chunk_results = await pipe.execute()
            for k, h in zip(batch, chunk_results):
                if not h:
                    continue
                h = {c: a for c, a in h.items() if c not in _EXCLUDED_CHAINS}
                if len(h) >= 2:
                    cg_id = k.split(":", 2)[-1]
                    groups[cg_id] = h

        blacklist: set[str] = await r.smembers("cc2_blacklist") or set()

        # ── Phase 1b: batch-fetch ALL prices via pipeline ────────────────
        all_contracts: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for group in groups.values():
            for chain, addr in group.items():
                if chain in _EXCLUDED_CHAINS:
                    continue
                k = (chain, addr.lower())
                if k not in seen:
                    seen.add(k)
                    all_contracts.append(k)

        prices_map: dict[tuple[str, str], float | None] = {}
        for i in range(0, len(all_contracts), CHUNK):
            batch = all_contracts[i : i + CHUNK]
            async with r.pipeline(transaction=False) as pipe:
                for chain, addr in batch:
                    pipe.get(f"cc2:price:{chain}:{addr}")
                results = await pipe.execute()
            for (chain, addr), v in zip(batch, results):
                try:
                    prices_map[(chain, addr)] = float(v) if v else None
                except (TypeError, ValueError):
                    prices_map[(chain, addr)] = None

        # ── Phase 1c: find candidates (pure memory, fast) ────────────────
        candidates: list[tuple[str, dict[str, str]]] = []
        for cg_id, group in groups.items():
            if cg_id.lower() in blacklist:
                continue
            if cg_id in structural:
                continue   # known persistent fake — wait for periodic recheck
            prices: list[float] = []
            for chain, addr in group.items():
                if chain in _EXCLUDED_CHAINS:
                    continue
                p = prices_map.get((chain, addr.lower()))
                if p and p > 0:
                    prices.append(p)
            if len(prices) < 2:
                continue
            cheap = min(prices)
            exp = max(prices)
            if cheap <= 0:
                continue
            spread = (exp - cheap) / cheap * 100

            # First time we ever price this token (this run): its current
            # spread is "pre-existing". If it's already huge, nobody
            # equalized it → it's structural junk, not a fresh arb. Park it
            # for 24h instead of alerting; the recheck loop releases it if
            # it later drops below threshold. This makes warmup per-token:
            # late-discovered tokens get the same treatment as startup ones.
            if cg_id not in self._seen_cg:
                self._seen_cg.add(cg_id)
                if spread >= self.STRUCTURAL_THRESHOLD:
                    await r.sadd("cc2_structural", cg_id)
                    log.info("cc_detector.parked_preexisting",
                             cg_id=cg_id, spread=round(spread, 1))
                    continue

            if self._min_profit <= spread <= self._max_profit:
                candidates.append((cg_id, group))
            # Note: cooldown auto-expires via TTL (1h). Previously we did
            # scan_iter("cc2_alerted:{cg_id}:*") here for every below-threshold
            # group — that's O(redis_keysize) PER GROUP, ~5000×80k = 400M scan
            # ops per cycle, hung _scan for hours. Removed.

        if not candidates:
            return {"candidates": 0, "alerted": 0}

        # ── Phase 2: parallel verification + alert ───────────────────────
        # 1000 paralleling — uses all 1000 proxies. Paired with TCPConnector(limit=1000)
        # in verifier and Redis max_connections=2000.
        sem = asyncio.Semaphore(1000)

        async def _process(cg_id: str, group: dict[str, str]) -> bool:
            async with sem:
                try:
                    return await self._check_group(cg_id, group)
                except Exception as e:
                    log.error("cc_detector.process_error", cg_id=cg_id, err=str(e))
                    return False

        results = await asyncio.gather(*[_process(c, g) for c, g in candidates])
        alerted = sum(1 for r in results if r)
        return {"candidates": len(candidates), "alerted": alerted}

    async def _check_group(self, cg_id: str, group: dict[str, str]) -> bool:
        r = await get_redis()
        # Respect blacklist — skip entirely
        if await r.sismember("cc2_blacklist", cg_id.lower()):
            return False

        prices: dict[str, tuple[str, float]] = {}
        for chain, addr in group.items():
            if chain in _EXCLUDED_CHAINS:
                continue
            price = await get_price(chain, addr)
            if price and price > 0:
                prices[chain] = (addr, price)

        if len(prices) < 2:
            return False

        # NOTE: median-based outlier filter was REMOVED — it killed real memecoin
        # pumps on thin pools (e.g. BONK polygon $4k liq, real 25× spread vs median
        # = real pump, dropped 22 times as "outlier"). Pool-quality validation
        # happens later in liquidity_and_volume_check via DS cross-check.

        sorted_prices = sorted(prices.items(), key=lambda kv: kv[1][1])

        # Build ALL (cheap, exp) pairs that meet min spread, sorted by spread desc.
        all_pairs: list[tuple[float, str, str, float, str, str, float]] = []
        for i in range(len(sorted_prices)):
            ch_chain, (ch_addr, ch_price) = sorted_prices[i]
            for j in range(len(sorted_prices) - 1, i, -1):
                ex_chain, (ex_addr, ex_price) = sorted_prices[j]
                if ex_price <= ch_price:
                    continue
                spread = (ex_price - ch_price) / ch_price * 100
                if spread < self._min_profit or spread > self._max_profit:
                    continue
                all_pairs.append((spread, ch_chain, ch_addr, ch_price,
                                  ex_chain, ex_addr, ex_price))
        all_pairs.sort(reverse=True)
        if not all_pairs:
            return False

        # Iterate ALL viable pairs bounded by a 15s deadline. Each per-pair
        # check (verify + liq gate) gets up to 8s. Broadcast is fire-and-
        # forget so even a slow Telegram send won't blow the budget.
        deadline = time.monotonic() + 15.0
        for pair in all_pairs:
            if time.monotonic() >= deadline:
                log.debug("cc_detector.pair_iter_deadline", cg_id=cg_id,
                          total_pairs=len(all_pairs))
                break
            try:
                ok = await asyncio.wait_for(self._try_pair(cg_id, *pair), timeout=8)
            except asyncio.TimeoutError:
                log.debug("cc_detector.try_pair_timeout", cg_id=cg_id)
                continue
            if ok:
                return True
        return False

    async def _try_pair(
        self, cg_id: str,
        gross: float,
        cheap_chain: str, cheap_addr: str, cheap_price: float,
        exp_chain: str, exp_addr: str, exp_price: float,
    ) -> bool:
        r = await get_redis()

        # Dynamic spread threshold: scales with expected slippage on smaller pool.
        async with r.pipeline(transaction=False) as pipe:
            pipe.get(f"cc2:liq_usd:{cheap_chain}:{cheap_addr.lower()}")
            pipe.get(f"cc2:liq_usd:{exp_chain}:{exp_addr.lower()}")
            liq_raw = await pipe.execute()
        def _f(v) -> float:
            try: return float(v) if v else 0.0
            except (TypeError, ValueError): return 0.0
        cheap_liq, exp_liq = _f(liq_raw[0]), _f(liq_raw[1])
        # Pass 0 when liq unknown — _dynamic_min_profit handles it as "no slip penalty".
        smaller_liq = min(cheap_liq, exp_liq)
        required_profit = self._dynamic_min_profit(smaller_liq)

        if gross < required_profit:
            log.debug("cc_detector.below_dynamic_threshold",
                      cg_id=cg_id, gross=round(gross, 2),
                      required=round(required_profit, 2),
                      smaller_liq=round(smaller_liq, 0))
            return False

        # Warmup
        if self._in_warmup():
            return False

        # ── Re-query verification BEFORE claiming dedup ──────────────────
        # Previously dedup was claimed at the top, then released in finally
        # if alerted=False. That made a hung/cancelled broadcast clear the
        # dedup, letting the same alert re-fire every cycle. Now we only
        # claim AFTER verify+liq pass — once claimed, the alert is real and
        # broadcast can't reset state.
        fresh = await self._verifier.verify(
            cheap_chain, cheap_addr, exp_chain, exp_addr,
        )
        if fresh is None:
            log.debug("cc_detector.verify_no_data", cg_id=cg_id)
            return False

        fresh_cheap, fresh_exp = fresh
        fresh_gross = (fresh_exp - fresh_cheap) / fresh_cheap * 100

        if fresh_gross < required_profit:
            log.info("cc_detector.verify_killed_stale",
                     cg_id=cg_id,
                     ds_gross=round(gross, 2),
                     fresh_gross=round(fresh_gross, 2),
                     required=round(required_profit, 2))
            return False

        if self._is_stable_or_bluechip(cg_id):
            min_liq_for_pair = self._alert_min_liq
        else:
            min_liq_for_pair = max(5000.0, self._alert_min_liq * 0.25)
        liq_ok, cheap_info, exp_info = await self._verifier.liquidity_and_volume_check(
            cheap_chain, cheap_addr,
            exp_chain, exp_addr,
            min_liq_for_pair,
            self._alert_min_vol,
            self._new_pool_max_age_h,
            self._new_pool_min_vol,
        )
        if not liq_ok:
            log.info("cc_detector.killed_low_liq_or_vol",
                     cg_id=cg_id,
                     min_liq=self._alert_min_liq,
                     min_vol=self._alert_min_vol)
            return False

        # All checks passed — atomically claim dedup. If another cycle
        # claimed first (race), bail.
        spread_bucket = int(fresh_gross / 5)
        dedup_key = f"cc2_alerted:{cg_id}:{cheap_chain}:{exp_chain}:b{spread_bucket}"
        claimed = await r.set(dedup_key, "1", ex=self._cooldown_sec, nx=True)
        if not claimed:
            return False

        cheap_price = fresh_cheap
        exp_price   = fresh_exp
        gross       = fresh_gross

        opp = CrossChainOpportunity(
            cg_id=cg_id,
            symbol=cg_id.replace("-", " ").title(),
            cheap_chain=cheap_chain, cheap_addr=cheap_addr, cheap_price=cheap_price,
            expensive_chain=exp_chain, expensive_addr=exp_addr, expensive_price=exp_price,
            gross_spread_pct=gross,
            bridge_cost_usd=0.0, bridge_cost_pct=0.0,
            net_profit_pct=gross, net_profit_usd=(gross / 100) * self._trade_size_usd,
            trade_size_usd=self._trade_size_usd,
            cheap_liq_usd=cheap_info["liquidity"] if cheap_info else None,
            cheap_vol_h24_usd=cheap_info["vol_h24"] if cheap_info else None,
            expensive_liq_usd=exp_info["liquidity"] if exp_info else None,
            expensive_vol_h24_usd=exp_info["vol_h24"] if exp_info else None,
        )
        log.info("cc_detector.opportunity", summary=opp.summary())
        # Fire-and-forget broadcast: detector cycle doesn't wait for Telegram.
        # If broadcast hangs/fails, dedup still holds (we already claimed).
        asyncio.create_task(self._on_opportunity(opp),
                            name=f"alert_{cg_id}")
        return True
