"""Compare price coverage: old bot (cc:*) vs new bot (cc2:*)."""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import redis.asyncio as r


async def main():
    cli = r.from_url("redis://localhost:6379/1", decode_responses=True)

    async def count(pattern):
        n = 0
        async for _ in cli.scan_iter(match=pattern, count=2000):
            n += 1
        return n

    old_addrs = await count("cg:contract:*")
    old_priced = await count("cc:price:*")
    old_pool_meta = await count("cc:pool_meta:*")

    new_addrs = await count("cg2:contract:*")
    new_priced = await count("cc2:price:*")

    print("=" * 60)
    print(" OLD BOT (cc:* / cg:* — paused, data frozen)")
    print("=" * 60)
    print(f"  Addresses in registry:    {old_addrs:>7,}")
    print(f"  cc:price set:             {old_priced:>7,}  ({old_priced*100//max(old_addrs,1)}%)")
    print(f"  cc:pool_meta (Alchemy):   {old_pool_meta:>7,}")

    print()
    print("=" * 60)
    print(" NEW BOT (cc2:* / cg2:* — running)")
    print("=" * 60)
    print(f"  Addresses in registry:    {new_addrs:>7,}")
    print(f"  cc2:price set:            {new_priced:>7,}  ({new_priced*100//max(new_addrs,1)}%)")

    print()
    print("=" * 60)
    print(" DELTA")
    print("=" * 60)
    print(f"  Priced addresses:  old={old_priced:,}  new={new_priced:,}  delta={new_priced - old_priced:+,}")

    await cli.aclose()


asyncio.run(main())
