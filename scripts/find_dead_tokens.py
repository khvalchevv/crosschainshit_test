"""
One-off registry audit: which token groups are DEAD — i.e. NOT a single
one of their contract addresses, on ANY chain, has a live trading pool
(DexScreener or GeckoTerminal), even at $0 liquidity.

This is intentionally slow & thorough (small batches, retries, modest
concurrency) — the opposite of the 10s monitor cycle — so a token is only
declared dead after it failed to price across several independent passes.
A group is ALIVE if >=1 address returns price > 0 even once.

Output:  data/dead_tokens.json
    { "generated_at", "checked_groups", "alive", "dead",
      "dead_ids": [ "wh-xxx", "cg-id", ... ],
      "dead_sample": [ {id, symbol, chains:[...]}... first 50 ] }

Run:  python scripts/find_dead_tokens.py
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import aiohttp

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import ROOT
from core.geckoterminal import GeckoTerminalClient
from core.monitor import _DS_CHAIN_SLUG, _EXCLUDED_CHAINS
from utils import get_redis, close_redis, get_proxy_manager, norm_addr, setup_logging

DS_BATCH = 30
CONCURRENCY = 60          # gentle — we want answers, not rate-limit bans
RETRY_ROUNDS = 3          # re-probe still-dead groups this many extra times


async def _ds_alive_batch(session, sem, proxies, slug, addrs):
    """Return set of addrs (norm) on `slug` that DS shows ANY pool with price>0."""
    url = "https://api.dexscreener.com/latest/dex/tokens/" + ",".join(addrs)
    async with sem:
        try:
            async with session.get(url, proxy=proxies.next()) as r:
                if r.status != 200:
                    return None  # unknown — retry later
                body = await r.json(content_type=None)
        except Exception:
            return None
    pairs = body.get("pairs") or []
    want = {norm_addr(a) for a in addrs}
    alive = set()
    for p in pairs:
        if (p.get("chainId") or "").lower() != slug:
            continue
        try:
            ba = norm_addr(p["baseToken"]["address"])
            price = float(p.get("priceUsd") or 0)
        except (KeyError, TypeError, ValueError):
            continue
        if price > 0 and ba in want:
            alive.add(ba)
    return alive


async def main() -> None:
    setup_logging()
    t0 = time.time()
    r = await get_redis()
    proxies = get_proxy_manager()
    gt = GeckoTerminalClient()

    # ── Load multichain groups ───────────────────────────────────────────
    gk = []
    async for k in r.scan_iter(match="cg2:group:*", count=1000):
        gk.append(k)
    groups: dict[str, dict[str, str]] = {}
    for i in range(0, len(gk), 1000):
        async with r.pipeline(transaction=False) as pipe:
            for k in gk[i:i + 1000]:
                pipe.hgetall(k)
            for k, h in zip(gk[i:i + 1000], await pipe.execute()):
                h = {c: a for c, a in (h or {}).items() if c not in _EXCLUDED_CHAINS}
                if len(h) >= 2:
                    groups[k.split(":", 2)[-1]] = h
    print(f"multichain groups to audit: {len(groups)}")

    # addr -> set(gid) reverse, and per-chain address universe
    alive_addr: set[tuple[str, str]] = set()

    sem = asyncio.Semaphore(CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=15)

    async def sweep_ds(targets: dict[str, list[str]]) -> None:
        async with aiohttp.ClientSession(timeout=timeout) as s:
            tasks = []
            for chain, addrs in targets.items():
                slug = _DS_CHAIN_SLUG.get(chain)
                if not slug:
                    continue
                for j in range(0, len(addrs), DS_BATCH):
                    batch = addrs[j:j + DS_BATCH]
                    tasks.append(_probe(s, sem, proxies, chain, slug, batch))
            await asyncio.gather(*tasks)

    async def _probe(s, sem, proxies, chain, slug, batch):
        res = await _ds_alive_batch(s, sem, proxies, slug, batch)
        if res:
            for a in res:
                alive_addr.add((chain, a))

    def dead_groups() -> dict[str, dict[str, str]]:
        out = {}
        for gid, g in groups.items():
            if not any((c, norm_addr(a)) in alive_addr for c, a in g.items()):
                out[gid] = g
        return out

    # ── Round 0: DS over everything ──────────────────────────────────────
    targets: dict[str, list[str]] = {}
    for g in groups.values():
        for c, a in g.items():
            targets.setdefault(c, []).append(a)
    for c in targets:
        targets[c] = list({norm_addr(x) for x in targets[c]})
    await sweep_ds(targets)
    print(f"after DS round 0: dead={len(dead_groups())}")

    # ── GT pass on whatever DS left dead ─────────────────────────────────
    dg = dead_groups()
    gt_q = []
    for g in dg.values():
        for c, a in g.items():
            gt_q.append((c, norm_addr(a)))
    if gt_q:
        gt_res = await gt.fetch_prices(gt_q)
        for (c, a), price in gt_res.items():
            if price and price > 0:
                alive_addr.add((c, norm_addr(a)))
    print(f"after GT pass: dead={len(dead_groups())}")

    # ── Retry rounds (rate-limit recovery) on still-dead only ────────────
    for rd in range(1, RETRY_ROUNDS + 1):
        dg = dead_groups()
        if not dg:
            break
        await asyncio.sleep(20)  # let rate-limit windows reset
        rt: dict[str, list[str]] = {}
        for g in dg.values():
            for c, a in g.items():
                rt.setdefault(c, []).append(norm_addr(a))
        for c in rt:
            rt[c] = list(set(rt[c]))
        await sweep_ds(rt)
        print(f"after retry {rd}: dead={len(dead_groups())}")

    # ── Result ───────────────────────────────────────────────────────────
    dg = dead_groups()
    dead_ids = sorted(dg.keys())
    sample = []
    for gid in dead_ids[:50]:
        sym = gid[3:] if gid.startswith("wh-") else gid
        sample.append({"id": gid, "symbol": sym,
                        "chains": sorted(dg[gid].keys())})

    out = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "checked_groups": len(groups),
        "alive": len(groups) - len(dg),
        "dead": len(dg),
        "dead_pct": round(100 * len(dg) / max(1, len(groups)), 1),
        "elapsed_sec": round(time.time() - t0, 1),
        "dead_ids": dead_ids,
        "dead_sample": sample,
    }
    path = ROOT / "data" / "dead_tokens.json"
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False),
                     encoding="utf-8")

    print("=" * 50)
    print(f"checked : {out['checked_groups']}")
    print(f"alive   : {out['alive']}")
    print(f"dead    : {out['dead']}  ({out['dead_pct']}%)")
    print(f"elapsed : {out['elapsed_sec']}s")
    print(f"written : {path}")

    await gt.close()
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
