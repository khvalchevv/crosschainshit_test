"""Diagnose why a specific token isn't being alerted."""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config import get_thresholds
from core.monitor import get_price
from utils import close_redis, get_redis


async def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python why_no_alert.py <cg_id>")
        return
    cg_id = sys.argv[1].lower()

    r = await get_redis()

    # 1. Blacklist
    is_bl = await r.sismember("cc2_blacklist", cg_id)
    print(f"Blacklisted: {is_bl}")

    # 2. Group contracts
    group = await r.hgetall(f"cg2:group:{cg_id}")
    if not group:
        print(f"ERROR: no cg2:group:{cg_id} — check that cg_id is correct")
        await close_redis()
        return

    print(f"\nChains in group: {len(group)}")
    for chain, addr in group.items():
        price = await get_price(chain, addr)
        print(f"  {chain:<12} {addr[:12]}...  ${price if price else '—'}")

    # 3. Find best spread
    prices = []
    for chain, addr in group.items():
        p = await get_price(chain, addr)
        if p and p > 0:
            prices.append((p, chain))
    prices.sort()
    if len(prices) >= 2:
        cheap_p, cheap_c = prices[0]
        exp_p, exp_c = prices[-1]
        spread = (exp_p - cheap_p) / cheap_p * 100
        print(f"\nBest spread: {cheap_c} ${cheap_p:.4f} -> {exp_c} ${exp_p:.4f} = {spread:.2f}%")

        cfg = get_thresholds()["arbitrage"]
        print(f"Threshold min: {cfg['min_profit_percent']}%  max: {cfg['max_profit_percent']}%")

        # 4. Check cooldown for this pair
        dedup_key = f"cc2_alerted:{cg_id}:{cheap_c}:{exp_c}"
        cooldown_ttl = await r.ttl(dedup_key)
        exists = await r.exists(dedup_key)
        if exists:
            hrs = cooldown_ttl / 3600
            print(f"\n🚫 IN COOLDOWN: cc2_alerted:{cg_id}:{cheap_c}:{exp_c}  expires in {hrs:.1f}h")
        else:
            print(f"\n✅ No cooldown for {cheap_c}→{exp_c}")

        # 5. Check first_seen (age filter)
        fs_key = f"cc_first_seen:{cg_id}:{cheap_c}:{exp_c}"
        fs = await r.get(fs_key)
        if fs:
            import time
            age_h = (time.time() - float(fs)) / 3600
            max_age_h = cfg.get("max_spread_age_sec", 7200) / 3600
            print(f"First seen {age_h:.2f}h ago (max {max_age_h}h)")
            if age_h > max_age_h:
                print(f"🚫 SUPPRESSED STALE: age > max_spread_age")
        else:
            print("No first_seen record — spread will be treated as new")
    else:
        print(f"\nOnly {len(prices)} chain(s) have prices — need ≥2")

    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
