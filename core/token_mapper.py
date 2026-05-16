"""
Builds a unified registry of tokens that exist on multiple chains.

Two sources, merged:
  1. CoinGecko coins/list (project pages — every contract of a project is
     linked together by the CoinGecko project id).
  2. data/all_bridged_tokens.json — pre-built Wormhole + LayerZero bridged
     token registry, keyed by symbol.

Merge rule (no group_merger needed):
  - CoinGecko groups are keyed by its project id   -> cg2:group:{cg_id}
  - For a bridged-JSON token: if ANY of its (chain, addr) already maps to an
    existing CoinGecko group -> append its addresses into THAT group.
    Otherwise create a standalone group  wh-{symbol}.
  This keeps CoinGecko's collision-safe grouping and only adds purely-bridged
  tokens CoinGecko doesn't index.

Redis keys:
    cg2:contract:{chain}:{addr}  -> group_id          (reverse lookup)
    cg2:group:{group_id}         -> hash {chain: addr, ...}
    cg2:bridges:{group_id}       -> set {wormhole, layerzero}
    cg2:refreshed_at             -> unix ts of last refresh
"""
from __future__ import annotations

import json
import time

import aiohttp

from config import ROOT, get_thresholds
from utils import get_logger, get_redis, norm_addr

log = get_logger(__name__)

# CoinGecko platform id → our chain name.
# Also reused to map the bridged-JSON platform names (same slug scheme).
CG_PLATFORM_MAP = {
    # ── Major EVM ─────────────────────────────────────────────────────
    "ethereum":                "ethereum",
    "binance-smart-chain":     "bsc",
    "polygon-pos":             "polygon",
    "arbitrum-one":            "arbitrum",
    "base":                    "base",
    "optimistic-ethereum":     "optimism",
    "avalanche":               "avalanche",
    "fantom":                  "fantom",
    "zksync":                  "zksync",
    "linea":                   "linea",
    "blast":                   "blast",
    "scroll":                  "scroll",
    "mantle":                  "mantle",
    "berachain":               "berachain",
    "celo":                    "celo",
    "cronos":                  "cronos",
    "moonbeam":                "moonbeam",
    "gnosis":                  "gnosis",
    "metis-andromeda":         "metis",
    "harmony-shard-0":         "harmony",
    "iotex":                   "iotex",
    "rootstock":               "rsk",
    "flare-network":           "flare",
    "manta-pacific":           "manta",
    "taiko":                   "taiko",
    "plume-network":           "plume",
    "shibarium":               "shibarium",
    "zircuit":                 "zircuit",
    "opbnb":                   "opbnb",
    "kava":                    "kava",
    "core":                    "core",
    "kaia":                    "kaia",
    "klay-token":              "kaia",
    "ronin":                   "ronin",
    "zero-network":            "zero",
    "abstract":                "abstract",
    "sonic":                   "sonic",
    "sophon":                  "sophon",
    "hyperliquid":             "hyperliquid",
    "monad":                   "monad",
    "pulsechain":              "pulsechain",
    "telos":                   "telos",
    "bahamut":                 "bahamut",
    "neon-evm":                "neon",
    "okex-chain":              "okex",
    "bitkub-chain":            "bitkub",
    "oasys":                   "oasys",
    # ── Non-EVM L1s ───────────────────────────────────────────────────
    "solana":                  "solana",
    "sui":                     "sui",
    "aptos":                   "aptos",
    "tron":                    "tron",
    "near-protocol":           "near",
    "tezos":                   "tezos",
    "the-open-network":        "ton",
    "ton":                     "ton",
    "sei-network":             "sei",
    "sei-v2":                  "sei",
    "celestia":                "celestia",
    "cardano":                 "cardano",
    "filecoin":                "filecoin",
    "osmosis":                 "osmosis",
    "neutron":                 "neutron",
    "injective":               "injective",
    "kujira":                  "kujira",
    "cosmos":                  "cosmos",
    "terra":                   "terra",
    "terra-2":                 "terra2",
    "archway":                 "archway",
    "dymension":               "dymension",
    "xrp":                     "xrp",
    "stellar":                 "stellar",
    "bitcoin-cash":            "bch",
    "litecoin":                "ltc",
    "dogecoin":                "doge",
    "algorand":                "algorand",
    "polkadot":                "polkadot",
    "kusama":                  "kusama",
    "elrond":                  "multiversx",
    "multiversx":              "multiversx",
    "hedera-hashgraph":        "hedera",
    "stacks":                  "stacks",
    "icon":                    "icon",
    "waves":                   "waves",
    "zilliqa":                 "zilliqa",
    "venom":                   "venom",
    "everscale":               "everscale",
    "alephium":                "alephium",
    "movement":                "movement",
}

_COINS_LIST_URL = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
_CACHE_TTL_SEC  = 7 * 86400


def _clean_addr(addr: str | None) -> str | None:
    """Reject junk addresses from the bridged JSON (peer-manager notes,
    placeholders). A real token contract has no spaces or parentheses."""
    if not addr:
        return None
    a = addr.strip()
    if not a or " " in a or "(" in a or ")" in a:
        return None
    return norm_addr(a)


class TokenMapper:
    async def refresh(self, force: bool = False) -> dict[str, int]:
        r = await get_redis()
        if not force:
            last = await r.get("cg2:refreshed_at")
            if last and time.time() - float(last) < _CACHE_TTL_SEC:
                age_h = round((time.time() - float(last)) / 3600, 1)
                log.info("token_mapper.cache_fresh", age_hours=age_h)
                return {"cached": 1}

        dead = self._load_dead()
        if dead:
            await self._purge_dead(dead)
        cg = await self._refresh_coingecko(dead)
        wh = await self._load_bridged_json(dead)
        await r.set("cg2:refreshed_at", str(time.time()))
        return {**{f"cg_{k}": v for k, v in cg.items()},
                **{f"wh_{k}": v for k, v in wh.items()},
                "dead_skipped": len(dead)}

    async def purge_registry(self) -> int:
        """Delete ONLY registry-namespace keys so a clean rebuild never
        touches user data. Explicitly never deletes cc2_blacklist,
        cc2_blacklist_leg, cc2_subscribers, cc2_alerted, cc2:price:*, etc.
        Use this instead of FLUSHDB."""
        r = await get_redis()
        patterns = ["cg2:group:*", "cg2:contract:*", "cg2:bridges:*",
                    "cg2:sym:*", "cg2:tkr:*", "cg2:refreshed_at"]
        deleted = 0
        for pat in patterns:
            batch: list[str] = []
            async for k in r.scan_iter(match=pat, count=1000):
                batch.append(k)
                if len(batch) >= 500:
                    deleted += await r.delete(*batch)
                    batch = []
            if batch:
                deleted += await r.delete(*batch)
        log.info("token_mapper.purged_registry", keys=deleted)
        return deleted

    @staticmethod
    def _load_dead() -> set[str]:
        """Group ids audited as DEAD (no pool on any chain) — skipped at
        build time so the registry only holds tradable tokens. Delete
        data/dead_tokens.json to re-include everything."""
        path = ROOT / "data" / "dead_tokens.json"
        try:
            with open(path, encoding="utf-8") as f:
                return set(json.load(f).get("dead_ids") or [])
        except Exception:
            return set()

    async def _purge_dead(self, dead: set[str]) -> None:
        """Drop dead groups (and their contract/bridge keys) left from a
        previous build so a refresh actually shrinks the registry."""
        r = await get_redis()
        removed = 0
        for i in range(0, len(dead), 500):
            chunk = list(dead)[i:i + 500]
            async with r.pipeline(transaction=False) as pipe:
                for gid in chunk:
                    pipe.hgetall(f"cg2:group:{gid}")
                hashes = await pipe.execute()
            async with r.pipeline(transaction=False) as pipe:
                for gid, h in zip(chunk, hashes):
                    for chain, addr in (h or {}).items():
                        pipe.delete(f"cg2:contract:{chain}:{norm_addr(addr)}")
                    pipe.delete(f"cg2:group:{gid}")
                    pipe.delete(f"cg2:bridges:{gid}")
                    pipe.delete(f"cg2:tkr:{gid}")
                    removed += 1
                await pipe.execute()
        log.info("token_mapper.purged_dead", groups=removed)

    async def _refresh_coingecko(self, dead: set[str] | None = None) -> dict[str, int]:
        dead = dead or set()
        log.info("token_mapper.cg_fetching")
        t0 = time.monotonic()
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60)
            ) as s:
                async with s.get(_COINS_LIST_URL) as resp:
                    if resp.status != 200:
                        log.error("token_mapper.cg_fail", status=resp.status)
                        return {"contracts": 0}
                    coins = await resp.json()
        except Exception as e:
            log.error("token_mapper.cg_error", err=str(e)[:100])
            return {"contracts": 0}

        log.info("token_mapper.cg_fetched", coins=len(coins),
                 sec=round(time.monotonic() - t0, 1))

        r = await get_redis()
        contracts = multichain = 0
        async with r.pipeline(transaction=False) as pipe:
            for coin in coins:
                cg_id = coin.get("id")
                platforms = coin.get("platforms") or {}
                if not cg_id or not platforms:
                    continue
                if cg_id in dead:
                    continue
                mapped = {
                    CG_PLATFORM_MAP[p]: norm_addr(a)
                    for p, a in platforms.items()
                    if a and p in CG_PLATFORM_MAP
                }
                if not mapped:
                    continue
                sym = (coin.get("symbol") or "").strip().lower()
                if sym:
                    pipe.sadd(f"cg2:sym:{sym}", cg_id)
                    pipe.set(f"cg2:tkr:{cg_id}", sym)
                pipe.hset(f"cg2:group:{cg_id}", mapping=mapped)
                if len(mapped) >= 2:
                    multichain += 1
                for chain, addr in mapped.items():
                    pipe.set(f"cg2:contract:{chain}:{addr}", cg_id)
                    contracts += 1
            await pipe.execute()
        log.info("token_mapper.cg_saved",
                 contracts=contracts, multichain=multichain)
        return {"contracts": contracts, "multichain": multichain}

    async def _load_bridged_json(self, dead: set[str] | None = None) -> dict[str, int]:
        """Load the Wormhole+LayerZero bridged registry and merge it into the
        CoinGecko groups (or create standalone wh-{symbol} groups)."""
        dead = dead or set()
        cfg = get_thresholds()["monitor"]
        path = ROOT / cfg.get("registry_file", "data/all_bridged_tokens.json")
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log.warning("token_mapper.bridged_json_err", err=str(e)[:120])
            return {"contracts": 0, "groups": 0}

        tokens = data.get("tokens") or {}
        r = await get_redis()
        groups = contracts = merged = 0

        for symbol, info in tokens.items():
            platforms = (info or {}).get("platforms") or {}
            mapped: dict[str, str] = {}
            bridges: set[str] = set()
            for plat, meta in platforms.items():
                chain = CG_PLATFORM_MAP.get(plat)
                if not chain:
                    continue
                addr = _clean_addr((meta or {}).get("address"))
                if not addr:
                    continue
                mapped[chain] = addr
                for b in (meta.get("bridges") or []):
                    bridges.add(str(b).lower())
            if len(mapped) < 2:
                continue

            # Attach to an existing CoinGecko group if any address is known.
            existing_gid: str | None = None
            async with r.pipeline(transaction=False) as pipe:
                for chain, addr in mapped.items():
                    pipe.get(f"cg2:contract:{chain}:{addr}")
                lookups = await pipe.execute()
            for gid in lookups:
                if gid:
                    existing_gid = gid.decode() if isinstance(gid, bytes) else gid
                    break

            target_gid = existing_gid or f"wh-{symbol.lower()}"
            if target_gid in dead:
                continue
            if existing_gid:
                merged += 1
            else:
                groups += 1

            async with r.pipeline(transaction=False) as pipe:
                pipe.hset(f"cg2:group:{target_gid}", mapping=mapped)
                pipe.sadd(f"cg2:sym:{symbol.lower()}", target_gid)
                pipe.set(f"cg2:tkr:{target_gid}", symbol.lower())
                for chain, addr in mapped.items():
                    # nx: never steal a contract already owned by a CG group
                    pipe.set(f"cg2:contract:{chain}:{addr}", target_gid, nx=True)
                    contracts += 1
                if bridges:
                    pipe.sadd(f"cg2:bridges:{target_gid}", *bridges)
                await pipe.execute()

        log.info("token_mapper.bridged_loaded",
                 new_groups=groups, merged_into_cg=merged, contracts=contracts)
        return {"contracts": contracts, "groups": groups, "merged": merged}
