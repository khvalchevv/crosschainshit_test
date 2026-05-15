"""
Builds a unified registry of tokens that exist on multiple chains.
Sources: CoinGecko + LayerZero OFT (free APIs).

Redis keys:
    cg2:contract:{chain}:{addr}  -> coingecko_id    (reverse lookup)
    cg2:group:{coingecko_id}     -> hash {chain: addr, ...}
    cg2:refreshed_at             -> unix ts of last refresh
"""
from __future__ import annotations

import time

import asyncio
import aiohttp

from utils import get_logger, get_proxy_manager, get_redis

log = get_logger(__name__)

# CoinGecko platform id → our chain name
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

_LZ_CHAIN_MAP = {
    "ethereum":   "ethereum",  "bsc":       "bsc",
    "polygon":    "polygon",   "arbitrum":  "arbitrum",
    "base":       "base",      "optimism":  "optimism",
    "avalanche":  "avalanche", "fantom":    "fantom",
    "zksync":     "zksync",    "linea":     "linea",
    "blast":      "blast",     "scroll":    "scroll",
    "mantle":     "mantle",    "berachain": "berachain",
    "solana":     "solana",    "aptos":     "aptos",
    "sui":        "sui",
}

_COINS_LIST_URL = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
_LZ_OFT_URL     = "https://metadata-api.layerzero-api.com/api/v1/tokens"
_CMC_LISTING_URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing"
_CMC_DETAIL_URL  = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail"
_CACHE_TTL_SEC  = 7 * 86400

# CoinMarketCap platform name → our chain name.
# CMC platform "name" field values taken from their API responses.
_CMC_PLATFORM_MAP = {
    # Major EVM
    "ethereum":                 "ethereum",
    "binance smart chain":      "bsc",
    "bnb smart chain (bep20)":  "bsc",
    "polygon":                  "polygon",
    "polygon pos":              "polygon",
    "arbitrum":                 "arbitrum",
    "arbitrum one":             "arbitrum",
    "base":                     "base",
    "optimism":                 "optimism",
    "avalanche c-chain":        "avalanche",
    "avalanche":                "avalanche",
    "fantom":                   "fantom",
    "zksync era":               "zksync",
    "zksync":                   "zksync",
    "linea":                    "linea",
    "blast":                    "blast",
    "scroll":                   "scroll",
    "mantle":                   "mantle",
    "berachain":                "berachain",
    "celo":                     "celo",
    "cronos":                   "cronos",
    "moonbeam":                 "moonbeam",
    "gnosis chain":             "gnosis",
    "xdai":                     "gnosis",
    "metis":                    "metis",
    "metis andromeda":          "metis",
    "harmony":                  "harmony",
    "iotex":                    "iotex",
    "rootstock":                "rsk",
    "flare":                    "flare",
    "manta pacific":            "manta",
    "manta":                    "manta",
    "taiko":                    "taiko",
    "plume":                    "plume",
    "shibarium":                "shibarium",
    "zircuit":                  "zircuit",
    "opbnb":                    "opbnb",
    "kava":                     "kava",
    "core":                     "core",
    "core dao":                 "core",
    "kaia":                     "kaia",
    "klaytn":                   "kaia",
    "ronin":                    "ronin",
    "zero network":             "zero",
    "abstract":                 "abstract",
    "sonic":                    "sonic",
    "sophon":                   "sophon",
    "hyperliquid":              "hyperliquid",
    "monad":                    "monad",
    "pulsechain":               "pulsechain",
    "telos":                    "telos",
    "bahamut":                  "bahamut",
    "neon evm":                 "neon",
    "oasys":                    "oasys",
    "bitkub chain":             "bitkub",
    "okex":                     "okex",
    # Non-EVM
    "solana":                   "solana",
    "sui":                      "sui",
    "aptos":                    "aptos",
    "tron20":                   "tron",
    "tron":                     "tron",
    "near":                     "near",
    "tezos":                    "tezos",
    "ton":                      "ton",
    "the open network":         "ton",
    "toncoin":                  "ton",
    "sei":                      "sei",
    "sei network":              "sei",
    "celestia":                 "celestia",
    "cardano":                  "cardano",
    "filecoin":                 "filecoin",
    "osmosis":                  "osmosis",
    "injective":                "injective",
    "cosmos":                   "cosmos",
    "xrp":                      "xrp",
    "xrp ledger":               "xrp",
    "stellar":                  "stellar",
    "dogecoin":                 "doge",
    "litecoin":                 "ltc",
    "bitcoin cash":             "bch",
    "algorand":                 "algorand",
    "polkadot":                 "polkadot",
    "hedera":                   "hedera",
    "hedera hashgraph":         "hedera",
    "stacks":                   "stacks",
    "multiversx":               "multiversx",
    "elrond":                   "multiversx",
}


class TokenMapper:
    async def refresh(self, force: bool = False, with_cmc: bool = False,
                      cmc_limit: int = 5000) -> dict[str, int]:
        """
        with_cmc=True runs CoinMarketCap discovery (slow, ~1-5 min).
        Off by default; trigger via script.
        """
        r = await get_redis()
        if not force:
            last = await r.get("cg2:refreshed_at")
            if last and time.time() - float(last) < _CACHE_TTL_SEC:
                age_h = round((time.time() - float(last)) / 3600, 1)
                log.info("token_mapper.cache_fresh", age_hours=age_h)
                return {"cached": 1}

        cg = await self._refresh_coingecko()
        lz = await self._refresh_layerzero()
        out = {**{f"cg_{k}": v for k, v in cg.items()},
               **{f"lz_{k}": v for k, v in lz.items()}}

        if with_cmc:
            cmc = await self._refresh_coinmarketcap(limit=cmc_limit)
            out.update({f"cmc_{k}": v for k, v in cmc.items()})

        await r.set("cg2:refreshed_at", str(time.time()))
        return out

    async def _refresh_coingecko(self) -> dict[str, int]:
        log.info("token_mapper.cg_fetching")
        t0 = time.monotonic()
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as s:
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
                mapped = {
                    CG_PLATFORM_MAP[p]: a.lower()
                    for p, a in platforms.items()
                    if a and p in CG_PLATFORM_MAP
                }
                if not mapped:
                    continue
                pipe.hset(f"cg2:group:{cg_id}", mapping=mapped)
                if len(mapped) >= 2:
                    multichain += 1
                for chain, addr in mapped.items():
                    pipe.set(f"cg2:contract:{chain}:{addr}", cg_id)
                    contracts += 1
            await pipe.execute()
        log.info("token_mapper.cg_saved", contracts=contracts, multichain=multichain)
        return {"contracts": contracts, "multichain": multichain}

    async def _refresh_coinmarketcap(self, limit: int = 5000) -> dict[str, int]:
        """
        CMC data-api (public, no key). Fetches top `limit` tokens by market cap,
        then pulls detail for each to get per-chain contract addresses.
        Uses proxy rotation for throughput.
        """
        log.info("token_mapper.cmc_fetching_slugs", limit=limit)
        t0 = time.monotonic()

        slugs = await self._cmc_fetch_slugs(limit)
        if not slugs:
            log.warning("token_mapper.cmc_no_slugs")
            return {"contracts": 0}

        log.info("token_mapper.cmc_got_slugs", count=len(slugs),
                 sec=round(time.monotonic() - t0, 1))

        # Fetch detail per slug in parallel via proxies
        proxies = get_proxy_manager()
        sem = asyncio.Semaphore(60)
        connector = aiohttp.TCPConnector(limit=200, limit_per_host=100)
        timeout = aiohttp.ClientTimeout(total=15)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async def _fetch_one(slug: str) -> dict | None:
                async with sem:
                    proxy = proxies.next()
                    try:
                        kwargs = {"proxy": proxy} if proxy else {}
                        async with session.get(
                            _CMC_DETAIL_URL, params={"slug": slug}, **kwargs
                        ) as resp:
                            if resp.status != 200:
                                return None
                            return await resp.json(content_type=None)
                    except Exception:
                        return None

            results = await asyncio.gather(*[_fetch_one(s) for s in slugs])

        r = await get_redis()
        total_contracts = 0
        total_multichain = 0
        merged_into_existing = 0

        for body in results:
            if not body:
                continue
            data = body.get("data") or {}
            slug = data.get("slug")
            if not slug:
                continue

            # Parse CMC platforms
            mapped: dict[str, str] = {}
            for p in (data.get("platforms") or []):
                plat = (p.get("contractPlatform") or "").strip().lower()
                addr = (p.get("contractAddress") or "").strip().lower()
                chain = _CMC_PLATFORM_MAP.get(plat)
                if chain and addr:
                    mapped[chain] = addr

            if not mapped:
                continue

            # Check if ANY of these (chain, addr) already points to an existing group
            existing_gid: str | None = None
            for chain, addr in mapped.items():
                gid = await r.get(f"cg2:contract:{chain}:{addr}")
                if gid:
                    existing_gid = gid
                    break

            target_gid = existing_gid or f"cmc-{slug}"
            if existing_gid:
                merged_into_existing += 1

            # Merge platforms into target group (adds missing chains if any)
            async with r.pipeline(transaction=False) as pipe:
                pipe.hset(f"cg2:group:{target_gid}", mapping=mapped)
                # Overwrite contract pointers to target group
                for chain, addr in mapped.items():
                    pipe.set(f"cg2:contract:{chain}:{addr}", target_gid)
                    total_contracts += 1
                await pipe.execute()

            # Check final group size (might have had more chains added)
            final = await r.hlen(f"cg2:group:{target_gid}")
            if final >= 2:
                total_multichain += 1

        log.info("token_mapper.cmc_saved",
                 contracts=total_contracts, multichain=total_multichain,
                 merged=merged_into_existing,
                 total_sec=round(time.monotonic() - t0, 1))
        return {"contracts": total_contracts, "multichain": total_multichain,
                "merged": merged_into_existing}

    async def _cmc_fetch_slugs(self, limit: int) -> list[str]:
        """Pull top N tokens by market cap via CMC listing endpoint."""
        proxies = get_proxy_manager()
        slugs: list[str] = []
        page_size = 1000

        connector = aiohttp.TCPConnector(limit=20)
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for start in range(1, limit + 1, page_size):
                proxy = proxies.next()
                params = {
                    "start": start,
                    "limit": min(page_size, limit - start + 1),
                    "sortBy": "market_cap",
                    "sortType": "desc",
                    "convert": "USD",
                    "cryptoType": "all",
                    "tagType": "all",
                    "audited": "false",
                }
                try:
                    kwargs = {"proxy": proxy} if proxy else {}
                    async with session.get(_CMC_LISTING_URL, params=params, **kwargs) as resp:
                        if resp.status != 200:
                            continue
                        body = await resp.json(content_type=None)
                        data = (body.get("data") or {}).get("cryptoCurrencyList") or []
                        for item in data:
                            slug = item.get("slug")
                            if slug:
                                slugs.append(slug)
                except Exception as e:
                    log.warning("cmc_listing_error", err=str(e)[:80])
        return slugs

    async def _refresh_layerzero(self) -> dict[str, int]:
        log.info("token_mapper.lz_fetching")
        t0 = time.monotonic()
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
                async with s.get(_LZ_OFT_URL) as resp:
                    if resp.status != 200:
                        log.warning("token_mapper.lz_fail", status=resp.status)
                        return {"contracts": 0}
                    data = await resp.json()
        except Exception as e:
            log.warning("token_mapper.lz_error", err=str(e)[:100])
            return {"contracts": 0}

        tokens = data.get("tokens", data) if isinstance(data, dict) else data
        if not isinstance(tokens, list):
            return {"contracts": 0}
        log.info("token_mapper.lz_fetched", tokens=len(tokens),
                 sec=round(time.monotonic() - t0, 1))

        r = await get_redis()
        added = 0
        async with r.pipeline(transaction=False) as pipe:
            for token in tokens:
                if not isinstance(token, dict):
                    continue
                symbol = (token.get("symbol") or "").lower()
                if not symbol:
                    continue
                deployments = token.get("deployments") or token.get("endpoints") or {}
                if not deployments:
                    continue
                mapped: dict[str, str] = {}
                for lz_chain, dep in deployments.items():
                    our = _LZ_CHAIN_MAP.get(lz_chain.lower())
                    if not our:
                        continue
                    addr = dep.get("address") if isinstance(dep, dict) else dep
                    if addr:
                        mapped[our] = addr.lower()
                if len(mapped) < 2:
                    continue
                gid = f"lz-{symbol}"
                pipe.hset(f"cg2:group:{gid}", mapping=mapped)
                for chain, addr in mapped.items():
                    pipe.set(f"cg2:contract:{chain}:{addr}", gid, nx=True)
                    added += 1
            await pipe.execute()
        log.info("token_mapper.lz_saved", contracts_added=added)
        return {"contracts": added}
