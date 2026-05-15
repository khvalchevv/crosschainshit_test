import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import redis.asyncio as r


async def main():
    cli = r.from_url("redis://localhost:6379/1", decode_responses=True)

    sol_total = 0
    sol_addrs = []
    async for k in cli.scan_iter(match="cg2:contract:solana:*", count=1000):
        sol_total += 1
        addr = k.split(":", 3)[3]
        sol_addrs.append(addr)

    pipe = cli.pipeline()
    for addr in sol_addrs:
        pipe.exists(f"cc2:price:solana:{addr}")
    res = await pipe.execute()
    sol_priced = sum(1 for x in res if x)

    print(f"Total Solana addresses in cg2:contract: {sol_total}")
    print(f"With cc2:price set: {sol_priced} ({sol_priced*100//max(sol_total,1)}%)")
    print()
    print("Sample stored addresses (note case):")
    for a in sol_addrs[:8]:
        print(f"  {a}")

    print()
    print("Sample groups (raw hash values):")
    cnt = 0
    async for gk in cli.scan_iter(match="cg2:group:*", count=1000):
        h = await cli.hgetall(gk)
        sol = h.get("solana")
        if sol:
            sym = gk.split(":", 2)[2]
            print(f"  {sym:30s}  -> {sol}")
            cnt += 1
            if cnt >= 8:
                break

    await cli.close()


asyncio.run(main())
