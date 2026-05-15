"""
Group merger — unifies duplicate token groups by detecting shared contracts.

Two groups that share ANY (chain, contract) pair are the same token and get
merged via union-find. Prefers bare-slug names (CoinGecko) as canonical;
deletes prefixed duplicates (cmc-, wh-, lz-, axelar-, lifi-, etc.).
"""
from __future__ import annotations

from utils import get_logger, get_redis

log = get_logger(__name__)

_TTL = 7 * 86400
_PREFIXES = ("cmc-", "wh-", "lz-", "axelar-", "squid-", "lifi-", "hop-", "synapse-")


def _is_better_canonical(a: str, b: str) -> bool:
    """Return True if `a` should be the kept id over `b`."""
    a_pref = any(a.startswith(p) for p in _PREFIXES)
    b_pref = any(b.startswith(p) for p in _PREFIXES)
    if a_pref and not b_pref:
        return False
    if b_pref and not a_pref:
        return True
    return len(a) < len(b)  # shorter wins on tie


async def merge_duplicate_groups() -> dict[str, int]:
    r = await get_redis()

    # 1. Snapshot all groups
    groups: dict[str, dict[str, str]] = {}
    async for key in r.scan_iter(match="cg2:group:*", count=1000):
        gid = key.split(":", 2)[-1]
        h = await r.hgetall(key)
        if h:
            # Normalize address case
            groups[gid] = {c: a.lower() for c, a in h.items()}

    if not groups:
        return {"merged": 0, "roots": 0}

    # 2. Inverse index: (chain, addr) → [gids]
    inv: dict[tuple[str, str], list[str]] = {}
    for gid, contracts in groups.items():
        for chain, addr in contracts.items():
            inv.setdefault((chain, addr), []).append(gid)

    # 3. Union-find
    parent = {gid: gid for gid in groups}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if _is_better_canonical(ra, rb):
            parent[rb] = ra
        else:
            parent[ra] = rb

    overlaps = 0
    for gids in inv.values():
        if len(gids) > 1:
            overlaps += 1
            for i in range(1, len(gids)):
                union(gids[0], gids[i])

    # 4. Collapse components
    components: dict[str, list[str]] = {}
    for gid in groups:
        components.setdefault(find(gid), []).append(gid)

    merged = 0
    for root, members in components.items():
        if len(members) <= 1:
            continue

        # Union all contracts + bridge tags
        all_contracts: dict[str, str] = {}
        all_bridges: set[str] = set()
        for m in members:
            all_contracts.update(groups[m])
            tags = await r.smembers(f"cg2:bridges:{m}")
            all_bridges.update(tags)

        async with r.pipeline(transaction=False) as pipe:
            # Write canonical group
            pipe.delete(f"cg2:group:{root}")  # clear stale keys so deletes are final
            pipe.hset(f"cg2:group:{root}", mapping=all_contracts)
            pipe.expire(f"cg2:group:{root}", _TTL)
            if all_bridges:
                pipe.delete(f"cg2:bridges:{root}")
                pipe.sadd(f"cg2:bridges:{root}", *all_bridges)
                pipe.expire(f"cg2:bridges:{root}", _TTL)
            # Redirect all contract pointers to root
            for chain, addr in all_contracts.items():
                pipe.setex(f"cg2:contract:{chain}:{addr}", _TTL, root)
            # Drop non-root members
            for m in members:
                if m != root:
                    pipe.delete(f"cg2:group:{m}")
                    pipe.delete(f"cg2:bridges:{m}")
                    merged += 1
            await pipe.execute()

    log.info("group_merger.done",
             total_groups=len(groups),
             overlapping_contracts=overlaps,
             merged_into_one=merged,
             final_groups=len(groups) - merged)
    return {
        "total_groups": len(groups),
        "overlapping_contracts": overlaps,
        "merged_into_one": merged,
        "final_groups": len(groups) - merged,
    }
