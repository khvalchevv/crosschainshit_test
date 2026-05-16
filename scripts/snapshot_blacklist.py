"""
One-off: snapshot every token that WOULD alert right now (spread in range +
both pools liquid enough) and drop it into cc2_blacklist.

Use before deploying so you start clean and only see NEW spreads form,
instead of the whole pre-existing backlog firing at once. Remove later with
`/blacklist clear` (or per token) when you want them back.

Requires the bot/monitor to have populated prices (run it while the bot is
up, or right after a monitor cycle).

    python scripts/snapshot_blacklist.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_thresholds
from core.monitor import _EXCLUDED_CHAINS
from utils import close_redis, get_redis, norm_addr, setup_logging


async def main() -> None:
    setup_logging()
    r = await get_redis()

    cfg = get_thresholds()["arbitrage"]
    mon = get_thresholds()["monitor"]
    min_p = cfg["min_profit_percent"]
    max_p = cfg["max_profit_percent"]
    min_liq = mon.get("alert_min_pool_liquidity_usd", 5000)

    already = await r.smembers("cc2_blacklist") or set()
    legbl = await r.smembers("cc2_blacklist_leg") or set()

    gk = []
    async for k in r.scan_iter(match="cg2:group:*", count=1000):
        gk.append(k)

    added = 0
    checked = 0
    for i in range(0, len(gk), 1000):
        chunk = gk[i:i + 1000]
        async with r.pipeline(transaction=False) as pipe:
            for k in chunk:
                pipe.hgetall(k)
            hashes = await pipe.execute()

        for k, h in zip(chunk, hashes):
            if not h:
                continue
            cg_id = k.split(":", 2)[-1]
            if cg_id.lower() in already:
                continue
            legs = {c: a for c, a in h.items()
                    if c not in _EXCLUDED_CHAINS
                    and f"{cg_id.lower()}@{c}" not in legbl}
            if len(legs) < 2:
                continue

            # prices
            async with r.pipeline(transaction=False) as pipe:
                for c, a in legs.items():
                    pipe.get(f"cc2:price:{c}:{norm_addr(a)}")
                pv = await pipe.execute()
            priced = []
            for (c, a), v in zip(legs.items(), pv):
                try:
                    p = float(v) if v else 0.0
                except (TypeError, ValueError):
                    p = 0.0
                if p > 0:
                    priced.append((c, a, p))
            if len(priced) < 2:
                continue
            checked += 1

            priced.sort(key=lambda x: x[2])
            (cc, ca, cp), (ec, ea, ep) = priced[0], priced[-1]
            spread = (ep - cp) / cp * 100
            if not (min_p <= spread <= max_p):
                continue

            async with r.pipeline(transaction=False) as pipe:
                pipe.get(f"cc2:liq_usd:{cc}:{norm_addr(ca)}")
                pipe.get(f"cc2:liq_usd:{ec}:{norm_addr(ea)}")
                lq = await pipe.execute()

            def _f(v):
                try:
                    return float(v) if v else 0.0
                except (TypeError, ValueError):
                    return 0.0
            if _f(lq[0]) < min_liq or _f(lq[1]) < min_liq:
                continue

            await r.sadd("cc2_blacklist", cg_id.lower())
            added += 1

    total = await r.scard("cc2_blacklist")
    print(f"groups with >=2 prices : {checked}")
    print(f"snapshot-blacklisted   : {added}")
    print(f"cc2_blacklist total now: {total}")
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
