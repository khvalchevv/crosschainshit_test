"""
Cross-chain spread detector.

Every detector_interval_sec:
  1. Load all groups + cached prices in bulk.
  2. For each group with >=2 prices: spread = (max - min) / min * 100.
  3. If min_profit <= spread <= max_profit AND both pools have cached
     liquidity >= alert_min_pool_liquidity_usd AND not on cooldown AND not
     blacklisted -> emit an opportunity.

No bridge-cost deduction (a separate bot handles bridge feasibility):
net_profit == gross_spread. No verifier re-query, no structural blacklist,
no dynamic thresholds — just the raw spread, a liquidity floor, and a
per-(token, chain-pair, spread-bucket) cooldown.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

from config import get_thresholds
from core.geckoterminal import GeckoTerminalClient
from core.monitor import _EXCLUDED_CHAINS
from utils import get_logger, get_redis, norm_addr

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
    cheap_liq_usd: float | None = None
    cheap_vol_h24_usd: float | None = None
    expensive_liq_usd: float | None = None
    expensive_vol_h24_usd: float | None = None
    spread_age_sec: float = 0.0
    ticker: str = ""
    detected_at: float = field(default_factory=time.time)

    def summary(self) -> str:
        return (
            f"{self.symbol} | BUY {self.cheap_chain} @ ${self.cheap_price:.6g} "
            f"-> SELL {self.expensive_chain} @ ${self.expensive_price:.6g} | "
            f"spread={self.gross_spread_pct:.2f}%"
        )


CrossChainCallback = Callable[[CrossChainOpportunity], Coroutine[Any, Any, None]]


def _symbol_of(cg_id: str) -> str:
    base = cg_id[3:] if cg_id.startswith("wh-") else cg_id
    return base.replace("-", " ").title()


def _median(xs: list[float]) -> float:
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _sane(price: float, med: float, factor: float) -> bool:
    """True if `price` is within [med/factor, med*factor] — i.e. not a
    junk/fabricated outlier vs the group median."""
    return med > 0 and (med / factor) <= price <= (med * factor)


class CrossChainDetector:
    def __init__(
        self,
        on_opportunity: CrossChainCallback,
        cycle_trigger: asyncio.Event | None = None,
    ) -> None:
        self._on_opportunity = on_opportunity
        self._running = False
        self._trigger = cycle_trigger

        cfg = get_thresholds()["arbitrage"]
        mon = get_thresholds()["monitor"]
        self._min_profit     : float = cfg["min_profit_percent"]
        self._max_profit     : float = cfg["max_profit_percent"]
        self._trade_size_usd : float = cfg.get("trade_size_usd", 10000)
        self._cooldown_sec   : int   = cfg["alert_cooldown_sec"]
        self._warmup_sec     : int   = cfg.get("warmup_sec", 60)
        self._interval_sec   : int   = mon.get("detector_interval_sec", 60)
        self._alert_min_liq  : float = mon.get(
            "alert_min_pool_liquidity_usd", 10000)
        # A leg whose price is >Nx (or <1/N) the group median is a junk /
        # fabricated-fallback price (e.g. GeckoTerminal inventing a price for
        # a chain with no real pool) — dropped before pairing.
        self._max_dev        : float = cfg.get("max_price_deviation_x", 5.0)
        self._gt = GeckoTerminalClient()
        self._started_at: float | None = None

    async def close(self) -> None:
        await self._gt.close()

    async def start(self) -> None:
        self._running = True
        self._started_at = time.time()
        log.info("cc_detector.started",
                 min_profit_pct=self._min_profit,
                 warmup_sec=self._warmup_sec)
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
            # Event-driven: wake the instant the monitor finishes writing a
            # fresh price sweep, instead of sleeping a fixed interval. The
            # timeout is just a safety net so a missed event (set/clear race
            # while we were mid-scan) can't stall detection longer than
            # _interval_sec.
            if self._trigger is not None:
                try:
                    await asyncio.wait_for(self._trigger.wait(),
                                           timeout=self._interval_sec)
                except asyncio.TimeoutError:
                    pass
            else:
                sleep_for = max(0, self._interval_sec - (time.monotonic() - t0))
                await asyncio.sleep(sleep_for)

    async def stop(self) -> None:
        self._running = False

    def _in_warmup(self) -> bool:
        if self._started_at is None:
            return True
        return (time.time() - self._started_at) < self._warmup_sec

    async def _scan(self) -> dict[str, int]:
        r = await get_redis()

        # ── Load all groups (bulk) ───────────────────────────────────────
        group_keys: list[str] = []
        async for key in r.scan_iter(match="cg2:group:*", count=1000):
            group_keys.append(key)
        if not group_keys:
            return {"candidates": 0, "alerted": 0}

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
        # Per-leg mutes: "{cg_id}@{chain}" — drop just that chain's price
        # from the group so the rest of the chains still produce spreads.
        legbl: set[str] = await r.smembers("cc2_blacklist_leg") or set()
        if legbl:
            for cg_id in list(groups):
                g = {c: a for c, a in groups[cg_id].items()
                     if f"{cg_id.lower()}@{c}" not in legbl}
                if len(g) >= 2:
                    groups[cg_id] = g
                else:
                    del groups[cg_id]

        # ── Bulk-fetch all prices ────────────────────────────────────────
        all_contracts: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for group in groups.values():
            for chain, addr in group.items():
                k = (chain, norm_addr(addr))
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

        # ── Find candidates ──────────────────────────────────────────────
        candidates: list[tuple[str, dict[str, str]]] = []
        for cg_id, group in groups.items():
            if cg_id.lower() in blacklist:
                continue
            prices: list[float] = []
            for chain, addr in group.items():
                p = prices_map.get((chain, norm_addr(addr)))
                if p and p > 0:
                    prices.append(p)
            if len(prices) < 2:
                continue
            # Drop price outliers vs median (junk/fabricated legs) so one bad
            # leg can't hide a real spread or flag a fake one. (n>=3 only —
            # median of 2 is meaningless; the 2-leg case is liquidity-vetted
            # in _try_group.)
            if len(prices) >= 3:
                med = _median(prices)
                prices = [p for p in prices if _sane(p, med, self._max_dev)]
                if len(prices) < 2:
                    continue
            cheap, exp = min(prices), max(prices)
            if cheap <= 0:
                continue
            spread = (exp - cheap) / cheap * 100
            if self._min_profit <= spread <= self._max_profit:
                candidates.append((cg_id, group))

        if not candidates or self._in_warmup():
            return {"candidates": len(candidates), "alerted": 0}

        sem = asyncio.Semaphore(200)

        async def _process(cg_id: str, group: dict[str, str]) -> bool:
            async with sem:
                try:
                    return await self._try_group(cg_id, group, prices_map)
                except Exception as e:
                    log.error("cc_detector.process_error",
                              cg_id=cg_id, err=str(e))
                    return False

        results = await asyncio.gather(
            *[_process(c, g) for c, g in candidates])
        return {"candidates": len(candidates),
                "alerted": sum(1 for x in results if x)}

    async def _try_group(
        self, cg_id: str, group: dict[str, str],
        prices_map: dict[tuple[str, str], float | None],
    ) -> bool:
        r = await get_redis()

        priced: list[tuple[str, str, float]] = []
        for chain, addr in group.items():
            p = prices_map.get((chain, norm_addr(addr)))
            if p and p > 0:
                priced.append((chain, addr, p))
        if len(priced) < 2:
            return False

        # Liquidity for ALL legs at once. Only legs with a real pool
        # (>= alert_min_liq) may be compared — this drops fabricated
        # fallback prices (e.g. GeckoTerminal inventing a price for a chain
        # with no pool) and ghost micro-pools, and lets a real spread
        # between two solid legs surface even when a junk leg exists.
        async with r.pipeline(transaction=False) as pipe:
            for c, a, _ in priced:
                pipe.get(f"cc2:liq_usd:{c}:{norm_addr(a)}")
            liq_raw = await pipe.execute()

        def _f(v) -> float:
            try:
                return float(v) if v else 0.0
            except (TypeError, ValueError):
                return 0.0

        liqs = [_f(lq) for lq in liq_raw]
        # Legs with no cached liq (GT-priced — the batch price endpoint has
        # no liquidity). Recover real liquidity via GT's per-token /pools
        # endpoint, but ONLY here (candidate legs, bounded + cached) so the
        # rate budget is fine. Otherwise legit GT legs get dropped as "?".
        gaps = [i for i, lv in enumerate(liqs) if lv <= 0]
        if gaps:
            recovered = await asyncio.gather(*[
                self._gt.fetch_liquidity(priced[i][0], priced[i][1])
                for i in gaps])
            for i, lv in zip(gaps, recovered):
                liqs[i] = lv

        liquid = [(c, a, p, lv)
                  for (c, a, p), lv in zip(priced, liqs)
                  if lv >= self._alert_min_liq]
        if len(liquid) < 2:
            log.debug("cc_detector.killed_low_liq", cg_id=cg_id,
                      liquid=len(liquid), min_liq=self._alert_min_liq)
            return False

        # Drop price outliers vs median of the liquid legs (n>=3).
        if len(liquid) >= 3:
            med = _median([x[2] for x in liquid])
            liquid = [x for x in liquid if _sane(x[2], med, self._max_dev)]
            if len(liquid) < 2:
                return False

        liquid.sort(key=lambda x: x[2])
        cheap_chain, cheap_addr, cheap_price, cheap_liq = liquid[0]
        exp_chain,   exp_addr,   exp_price,   exp_liq   = liquid[-1]
        spread = (exp_price - cheap_price) / cheap_price * 100
        if not (self._min_profit <= spread <= self._max_profit):
            return False

        # Spread age: first time this (token, cheap→exp) crossed the
        # threshold. The key is kept alive (TTL refreshed) every cycle the
        # spread holds; if it breaks for >60s the key expires and the age
        # restarts from zero next time. Resolution ~ detector interval (~5s).
        now = time.time()
        since_key = f"cc2:spread_since:{cg_id}:{cheap_chain}:{exp_chain}"
        prev = await r.get(since_key)
        try:
            started = float(prev) if prev else now
        except (TypeError, ValueError):
            started = now
        await r.set(since_key, str(started), ex=60)
        spread_age = max(0.0, now - started)

        tkr_raw = await r.get(f"cg2:tkr:{cg_id}")
        if tkr_raw:
            ticker = (tkr_raw.decode() if isinstance(tkr_raw, bytes)
                      else tkr_raw)
        else:
            ticker = cg_id[3:] if cg_id.startswith("wh-") else cg_id

        # Cooldown — bucketed by 5% spread so a meaningfully bigger jump
        # re-alerts within the window.
        bucket = int(spread / 5)
        dedup_key = (f"cc2_alerted:{cg_id}:{cheap_chain}:{exp_chain}:b{bucket}")
        claimed = await r.set(dedup_key, "1",
                              ex=self._cooldown_sec, nx=True)
        if not claimed:
            return False

        opp = CrossChainOpportunity(
            cg_id=cg_id,
            symbol=_symbol_of(cg_id),
            cheap_chain=cheap_chain, cheap_addr=cheap_addr,
            cheap_price=cheap_price,
            expensive_chain=exp_chain, expensive_addr=exp_addr,
            expensive_price=exp_price,
            gross_spread_pct=spread,
            bridge_cost_usd=0.0, bridge_cost_pct=0.0,
            net_profit_pct=spread,
            net_profit_usd=(spread / 100) * self._trade_size_usd,
            trade_size_usd=self._trade_size_usd,
            cheap_liq_usd=cheap_liq or None,
            expensive_liq_usd=exp_liq or None,
            spread_age_sec=spread_age,
            ticker=ticker,
        )
        log.info("cc_detector.opportunity", summary=opp.summary())
        asyncio.create_task(self._on_opportunity(opp), name=f"alert_{cg_id}")
        return True
