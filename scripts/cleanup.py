"""
Redis cleanup tool.

Usage:
    python scripts/cleanup.py --mode <single-chain|dead|all>
    python scripts/cleanup.py --dry-run         # just count, don't delete

Modes:
    single-chain  groups with <2 supported chains (deprecated single-chain
                  imports — bot can't arb them anyway)
    dead          (chain, addr) entries with no cc2:price
                  (CMC/LiFi/RWA imports that have no DEX presence)
    all           run dead then single-chain in sequence
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from utils import close_redis, get_redis, setup_logging

_CHAIN_SLUG = {
    "ethereum","bsc","polygon","arbitrum","base","optimism","avalanche",
    "zksync","linea","blast","scroll","mantle","berachain","celo","cronos",
    "moonbeam","metis","harmony","iotex","rsk","flare","manta","taiko","plume",
    "shibarium","zircuit","opbnb","kava","core","kaia","ronin","zero",
    "abstract","sonic","sophon","hyperliquid","monad","pulsechain","telos",
    "neon","oasys","bitkub","okex","solana","sui","aptos","tron","near","ton",
    "sei","cardano","filecoin","osmosis","injective","xrp",
}


# ── single-chain ──────────────────────────────────────────────────────────────

async def wipe_single_chain(dry_run: bool) -> None:
    r = await get_redis()
    keys: list[str] = []
    async for key in r.scan_iter(match="cg2:group:*", count=1000):
        keys.append(key.decode() if isinstance(key, bytes) else key)
    print(f"[single-chain] scanning {len(keys)} groups…")

    to_drop_groups: list[str] = []
    to_drop_contracts: list[str] = []
    multi = 0
    CHUNK = 1000
    for i in range(0, len(keys), CHUNK):
        batch = keys[i : i + CHUNK]
        async with r.pipeline(transaction=False) as pipe:
            for k in batch:
                pipe.hgetall(k)
            results = await pipe.execute()
        for k, h in zip(batch, results):
            if not h:
                to_drop_groups.append(k)
                continue
            slug = {c: a for c, a in h.items() if c in _CHAIN_SLUG}
            if len(slug) >= 2:
                multi += 1
                continue
            to_drop_groups.append(k)
            for c, a in h.items():
                to_drop_contracts.append(f"cg2:contract:{c}:{a.lower()}")

    print(f"[single-chain] keep={multi}  drop_groups={len(to_drop_groups)}  drop_contract_ptrs={len(to_drop_contracts)}")
    if dry_run:
        return
    for i in range(0, len(to_drop_groups), 500):
        await r.delete(*to_drop_groups[i : i + 500])
    for i in range(0, len(to_drop_contracts), 500):
        await r.delete(*to_drop_contracts[i : i + 500])
    print(f"[single-chain] deleted")


# ── dead tokens ───────────────────────────────────────────────────────────────

async def wipe_dead(dry_run: bool) -> None:
    r = await get_redis()
    keys: list[str] = []
    async for key in r.scan_iter(match="cg2:group:*", count=1000):
        keys.append(key.decode() if isinstance(key, bytes) else key)
    print(f"[dead] scanning {len(keys)} groups…")

    groups: dict[str, dict[str, str]] = {}
    CHUNK = 1000
    for i in range(0, len(keys), CHUNK):
        batch = keys[i : i + CHUNK]
        async with r.pipeline(transaction=False) as pipe:
            for k in batch:
                pipe.hgetall(k)
            for k, h in zip(batch, await pipe.execute()):
                if h:
                    groups[k] = {c: a.lower() for c, a in h.items()}

    all_pairs: list[tuple[str, str, str]] = [
        (k, c, a) for k, h in groups.items() for c, a in h.items()
    ]
    async with r.pipeline(transaction=False) as pipe:
        for _, c, a in all_pairs: pipe.exists(f"cc2:price:{c}:{a}")
        prices = await pipe.execute()

    dead_entries: dict[str, list[str]] = defaultdict(list)
    for (k, c, a), hp in zip(all_pairs, prices):
        if not hp:
            dead_entries[k].append(c)
    total = sum(len(v) for v in dead_entries.values())
    print(f"[dead] groups affected={len(dead_entries)}  dead_entries={total}")
    if dry_run:
        return

    contracts_removed = 0
    async with r.pipeline(transaction=False) as pipe:
        for k, dead_chains in dead_entries.items():
            pipe.hdel(k, *dead_chains)
            for c in dead_chains:
                addr = groups[k].get(c)
                if addr:
                    pipe.delete(f"cg2:contract:{c}:{addr}")
                    contracts_removed += 1
        await pipe.execute()
    print(f"[dead] removed {contracts_removed} contract pointers")
    # Drop now-orphan single-chain groups
    await wipe_single_chain(dry_run=False)


# ── orchestration ─────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="Redis cleanup tool")
    parser.add_argument(
        "--mode", required=True,
        choices=["single-chain", "dead", "all"],
        help="Cleanup mode",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Only count, don't delete")
    args = parser.parse_args()

    setup_logging()
    if args.mode == "single-chain":
        await wipe_single_chain(args.dry_run)
    elif args.mode == "dead":
        await wipe_dead(args.dry_run)
    elif args.mode == "all":
        await wipe_dead(args.dry_run)
    await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
