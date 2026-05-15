"""Full coverage statistics: per-chain tokens, price sources, candidate counts."""
from __future__ import annotations

import asyncio
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config import get_thresholds
from utils import close_redis, get_redis


async def main() -> None:
    r = await get_redis()

    # ── Scan all groups ──────────────────────────────────────────────────
    groups: dict[str, dict[str, str]] = {}
    async for key in r.scan_iter(match="cg2:group:*", count=1000):
        h = await r.hgetall(key)
        if h:
            groups[key.split(":", 2)[-1]] = h

    total_groups = len(groups)
    multichain = sum(1 for g in groups.values() if len(g) >= 2)

    all_pairs: set[tuple[str, str]] = set()
    per_chain_tokens: Counter = Counter()
    per_chain_multichain: Counter = Counter()

    for cg_id, group in groups.items():
        is_multi = len(group) >= 2
        for chain, addr in group.items():
            t = (chain, addr.lower())
            all_pairs.add(t)
            per_chain_tokens[chain] += 1
            if is_multi:
                per_chain_multichain[chain] += 1

    print("=" * 85)
    print(" OVERALL")
    print("=" * 85)
    print(f"  Token groups:                  {total_groups:>7,}")
    print(f"  Multichain (≥2 chains):        {multichain:>7,}")
    print(f"  Unique (chain,addr) pairs:     {len(all_pairs):>7,}")

    fresh_price_count: Counter = Counter()
    liq_count: Counter = Counter()

    async for k in r.scan_iter(match="cc2:price:*", count=1000):
        parts = k.split(":")
        if len(parts) >= 3:
            fresh_price_count[parts[2]] += 1

    async for k in r.scan_iter(match="cc2:liq_usd:*", count=1000):
        parts = k.split(":")
        if len(parts) >= 3:
            liq_count[parts[2]] += 1

    # ── Per-chain table ──────────────────────────────────────────────────
    print("\n" + "=" * 85)
    print(" PER-CHAIN COVERAGE")
    print("=" * 85)
    print(f"  {'CHAIN':<15} {'TOKENS':>7} {'MULTI':>7} {'PRICED':>14} {'LIQ':>7}")
    print(f"  {'-'*15} {'-'*7} {'-'*7} {'-'*14} {'-'*7}")
    all_chains = sorted(per_chain_tokens.keys(), key=lambda c: -per_chain_tokens[c])
    for chain in all_chains:
        n_tokens = per_chain_tokens[chain]
        n_multi = per_chain_multichain[chain]
        n_fresh = fresh_price_count.get(chain, 0)
        n_liq = liq_count.get(chain, 0)
        pct_fresh = f"{n_fresh*100//n_tokens}%" if n_tokens else "0%"
        print(f"  {chain:<15} {n_tokens:>7,} {n_multi:>7,} "
              f"{n_fresh:>6,}({pct_fresh:>4}) {n_liq:>7,}")

    # ── Candidate logic explanation ──────────────────────────────────────
    cfg = get_thresholds()["arbitrage"]
    mcfg = get_thresholds()["monitor"]
    print("\n" + "=" * 85)
    print(" CANDIDATE LOGIC (how detector decides to ALERT)")
    print("=" * 85)
    print(f"  Every {mcfg['interval_sec']}s:")
    print(f"    1. Load ALL {multichain:,} multichain groups from Redis")
    print(f"    2. For each → min/max price across chains")
    print(f"    3. spread% = (max-min)/min * 100")
    print(f"    4. Keep candidate if:  {cfg['min_profit_percent']}% ≤ spread ≤ {cfg['max_profit_percent']}%")
    print(f"    5. Skip if in cooldown ({cfg['alert_cooldown_sec']//3600}h) OR too old ({cfg.get('max_spread_age_sec',7200)//3600}h)")
    print(f"    6. Verifier: re-query DS after {mcfg.get('verify_delay_sec', 1)}s to filter stale")
    print(f"    7. Liq/vol gate: both sides ≥ ${mcfg.get('alert_min_pool_liquidity_usd',1000):,} liq "
          f"AND ≥ ${mcfg.get('alert_min_pool_volume_usd',1000):,} vol")
    if cfg.get("kyberswap_verify_enabled"):
        print(f"    8. KyberSwap aggregator quote per side — kill if no route or "
              f"slippage > {cfg.get('kyberswap_max_slippage_pct', 5)}%")
    print(f"    final. Alert sent → cooldown key set for {cfg['alert_cooldown_sec']//3600}h")

    # Candidate count now
    async def _price(c, a):
        v = await r.get(f"cc2:price:{c}:{a}")
        try: return float(v) if v else None
        except: return None

    cands = []
    for cg_id, group in groups.items():
        if len(group) < 2: continue
        prices = []
        for chain, addr in group.items():
            p = await _price(chain, addr.lower())
            if p and p > 0:
                prices.append((p, chain))
        if len(prices) < 2: continue
        prices.sort()
        sp = (prices[-1][0] - prices[0][0]) / prices[0][0] * 100
        if cfg['min_profit_percent'] <= sp <= cfg['max_profit_percent']:
            cands.append((sp, cg_id, prices[0][1], prices[-1][1]))

    print(f"\n  RIGHT NOW: {len(cands):,} candidates pass spread filter")
    cands.sort(reverse=True)
    for sp, cg_id, c1, c2 in cands[:10]:
        print(f"    {sp:>6.2f}%  {cg_id:<35} {c1} -> {c2}")

    print("\n" + "=" * 85)
    print(" PRICE SOURCE PIPELINE")
    print("=" * 85)
    print("  Phase A:  DefiLlama batch (all chains, ~80 parallel batches of 100)")
    print("  Phase B:  DexScreener + GeckoTerminal + OKX search + Jupiter (Solana)")
    print("            in parallel for whatever Phase A missed")
    print("  Verifier: DexScreener fresh re-query → DefiLlama fallback")
    if cfg.get("kyberswap_verify_enabled"):
        print("  Last:     KyberSwap aggregator route check (execution-grade)")

    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
