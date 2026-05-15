"""
Fetchers for bridge-specific token registries.
Each source expands our cg2:group:* mappings with tokens that bridges
support natively — often NOT in CoinGecko/CMC.

Data is merged into Redis:
    cg2:group:{gid}     → hash of {chain: addr}
    cg2:bridges:{gid}   → SET of bridge names this token is known to support
    cg2:contract:{chain}:{addr} → {gid}
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from utils import get_logger, get_proxy_manager, get_redis

log = get_logger(__name__)

_CACHE_TTL_SEC = 7 * 86400

# Map bridge-specific chain identifiers → our internal name
_BRIDGE_CHAIN_MAP = {
    # Common variants
    "ethereum":              "ethereum",
    "eth":                   "ethereum",
    "mainnet":               "ethereum",
    "bsc":                   "bsc",
    "binance":               "bsc",
    "bnb":                   "bsc",
    "bnb_smart_chain":       "bsc",
    "polygon":               "polygon",
    "polygon-pos":           "polygon",
    "matic":                 "polygon",
    "arbitrum":              "arbitrum",
    "arbitrum-one":          "arbitrum",
    "arbitrum_one":          "arbitrum",
    "base":                  "base",
    "optimism":              "optimism",
    "avalanche":             "avalanche",
    "avax":                  "avalanche",
    "fantom":                "fantom",
    "ftm":                   "fantom",
    "zksync":                "zksync",
    "zksync-era":            "zksync",
    "zksync_era":            "zksync",
    "linea":                 "linea",
    "blast":                 "blast",
    "scroll":                "scroll",
    "mantle":                "mantle",
    "berachain":             "berachain",
    "celo":                  "celo",
    "cronos":                "cronos",
    "moonbeam":              "moonbeam",
    "gnosis":                "gnosis",
    "xdai":                  "gnosis",
    "solana":                "solana",
    "sol":                   "solana",
    "sui":                   "sui",
    "aptos":                 "aptos",
    "tron":                  "tron",
    "near":                  "near",
    "tezos":                 "tezos",
}


# Map chain IDs (numeric) → our internal name
_CHAIN_ID_MAP = {
    1:       "ethereum",
    56:      "bsc",
    137:     "polygon",
    42161:   "arbitrum",
    8453:    "base",
    10:      "optimism",
    43114:   "avalanche",
    250:     "fantom",
    324:     "zksync",
    59144:   "linea",
    81457:   "blast",
    534352:  "scroll",
    5000:    "mantle",
    80094:   "berachain",
    42220:   "celo",
    25:      "cronos",
    1284:    "moonbeam",
    100:     "gnosis",
    480:     "worldchain",
    130:     "unichain",
    1868:    "soneium",
    34443:   "mode",
    7777777: "zora",
    1135:    "lisk",
    57073:   "ink",
    999:     "hyperliquid",
    146:     "sonic",
    2741:    "abstract",
    98866:   "plume",
    50104:   "sophon",
    33139:   "apechain",
    1116:    "core",
    11820:   "monad",
    369:     "pulsechain",
}


def _normalize_chain(s: str | int) -> str | None:
    if isinstance(s, int):
        return _CHAIN_ID_MAP.get(s)
    if isinstance(s, str):
        s2 = s.strip().lower().replace(" ", "_").replace("-", "_")
        return _BRIDGE_CHAIN_MAP.get(s2)
    return None


async def _save_group(
    r,
    mapped: dict[str, str],
    bridge_name: str,
    fallback_gid: str,
) -> bool:
    """Merge mapped platforms into existing group (if any contract overlaps)
    or create a new one. Tags group with bridge_name. Returns True if saved."""
    if not mapped:
        return False

    # Find existing group via any shared contract
    existing_gid: str | None = None
    for chain, addr in mapped.items():
        gid = await r.get(f"cg2:contract:{chain}:{addr.lower()}")
        if gid:
            existing_gid = gid
            break

    target_gid = existing_gid or fallback_gid

    async with r.pipeline(transaction=False) as pipe:
        pipe.hset(f"cg2:group:{target_gid}",
                  mapping={c: a.lower() for c, a in mapped.items()})
        pipe.sadd(f"cg2:bridges:{target_gid}", bridge_name)
        for chain, addr in mapped.items():
            pipe.set(f"cg2:contract:{chain}:{addr.lower()}", target_gid)
        await pipe.execute()
    return True


# ── LiFi (bridge aggregator, replaces Squid which blocks proxies) ──────────

async def refresh_lifi() -> dict[str, int]:
    """LiFi: https://li.quest/v1/tokens — returns per-chain token lists."""
    url = "https://li.quest/v1/tokens"
    return await _fetch_and_save(url, "lifi", _parse_lifi)


def _parse_lifi(body: dict) -> list[tuple[str, dict[str, str]]]:
    """
    LiFi format: {tokens: {chainId: [{symbol, address, coinKey, ...}]}}
    coinKey is effectively CoinGecko id-ish.
    """
    chains_obj = body.get("tokens") or {}
    groups: dict[str, dict[str, str]] = {}
    for cid_str, tokens in chains_obj.items():
        try:
            cid = int(cid_str)
        except (TypeError, ValueError):
            continue
        chain = _normalize_chain(cid)
        if not chain or not isinstance(tokens, list):
            continue
        for t in tokens:
            if not isinstance(t, dict):
                continue
            sym = (t.get("symbol") or "").lower()
            addr = (t.get("address") or "").lower()
            if not sym or not addr.startswith("0x"):
                continue
            key = (t.get("coinKey") or sym).lower()
            groups.setdefault(f"lifi-{key}", {})[chain] = addr
    return [(gid, m) for gid, m in groups.items() if m]


# ── Axelar ────────────────────────────────────────────────────────────────────

async def refresh_axelar() -> dict[str, int]:
    """Axelar: https://api.axelarscan.io/api/getAssets"""
    url = "https://api.axelarscan.io/api/getAssets"
    return await _fetch_and_save(url, "axelar", _parse_axelar)


def _parse_axelar(body: Any) -> list[tuple[str, dict[str, str]]]:
    # Axelar returns a list of assets with "addresses": {chainId: {address, ...}}
    assets = body if isinstance(body, list) else body.get("data") or []
    groups: list[tuple[str, dict[str, str]]] = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        sym = (asset.get("symbol") or "").lower()
        if not sym:
            continue
        addrs = asset.get("addresses") or {}
        mapped: dict[str, str] = {}
        for chain_name, data in addrs.items():
            chain = _normalize_chain(chain_name)
            if not chain:
                continue
            addr = None
            if isinstance(data, dict):
                addr = data.get("address") or data.get("ibc_denom")
            elif isinstance(data, str):
                addr = data
            if addr and addr.startswith("0x"):
                mapped[chain] = addr.lower()
        if mapped:
            groups.append((f"axelar-{sym}", mapped))
    return groups


# ── Wormhole ──────────────────────────────────────────────────────────────────

async def refresh_wormhole() -> dict[str, int]:
    """Wormhole token list (CSV format)."""
    url = "https://raw.githubusercontent.com/wormhole-foundation/wormhole-token-list/main/content/by_source.csv"
    return await _fetch_and_save(url, "wormhole", _parse_wormhole, is_json=False)


# CSV column → our chain name (for Wormhole by_source.csv)
_WH_CSV_CHAIN_COLS = {
    "solAddress":      "solana",
    "ethAddress":      "ethereum",
    "bscAddress":      "bsc",
    "maticAddress":    "polygon",
    "avaxAddress":     "avalanche",
    "ftmAddress":      "fantom",
    "celoAddress":     "celo",
    "nearAddress":     "near",
    "moonbeamAddress": "moonbeam",
    "optimismAddress": "optimism",
    "arbitrumAddress": "arbitrum",
    "aptosAddress":    "aptos",
    "baseAddress":     "base",
    "auroraAddress":   "aurora",
    "klaytnAddress":   "klaytn",
}


def _parse_wormhole(body: str) -> list[tuple[str, dict[str, str]]]:
    """Parse Wormhole CSV — each row = one bridged token with per-chain columns."""
    import csv
    import io

    groups: list[tuple[str, dict[str, str]]] = []
    reader = csv.DictReader(io.StringIO(body))
    for row in reader:
        sym = (row.get("symbol") or "").lower()
        if not sym:
            continue

        mapped: dict[str, str] = {}
        for col, chain in _WH_CSV_CHAIN_COLS.items():
            addr = (row.get(col) or "").strip().lower()
            if addr.startswith("0x") and len(addr) == 42:
                mapped[chain] = addr
            elif chain == "solana" and addr and not addr.startswith("0x"):
                mapped[chain] = addr  # Solana uses base58

        if len(mapped) >= 2:
            cg = (row.get("coingeckoId") or "").strip().lower()
            gid = f"wh-{cg}" if cg else f"wh-{sym}"
            groups.append((gid, mapped))
    return groups


# ── Across Protocol ───────────────────────────────────────────────────────────

async def refresh_across() -> dict[str, int]:
    """Across: https://app.across.to/api/available-routes — canonical L2 bridge routes."""
    url = "https://app.across.to/api/available-routes"
    return await _fetch_and_save(url, "across", _parse_across)


def _parse_across(body: Any) -> list[tuple[str, dict[str, str]]]:
    """
    Across returns a list of routes:
    [{originChainId, originToken, destinationChainId, destinationToken,
      originTokenSymbol, destinationTokenSymbol, isNative}, ...]
    For each route we register BOTH sides under symbol-keyed group,
    so any token that appears on ≥2 chains ends up multichain.
    """
    if not isinstance(body, list):
        return []
    groups: dict[str, dict[str, str]] = {}
    for route in body:
        if not isinstance(route, dict):
            continue
        for side in ("origin", "destination"):
            cid = route.get(f"{side}ChainId")
            tok = (route.get(f"{side}Token") or "").lower()
            sym = (route.get(f"{side}TokenSymbol") or "").lower()
            chain = _normalize_chain(cid) if isinstance(cid, int) else None
            if chain and tok.startswith("0x") and len(tok) == 42 and sym:
                groups.setdefault(f"across-{sym}", {})[chain] = tok
    return [(gid, m) for gid, m in groups.items() if len(m) >= 2]


# ── Symbiosis Finance ─────────────────────────────────────────────────────────

async def refresh_symbiosis() -> dict[str, int]:
    """Symbiosis: cross-chain DEX with curated token list."""
    url = "https://api.symbiosis.finance/crosschain/v1/tokens"
    return await _fetch_and_save(url, "symbiosis", _parse_symbiosis)


def _parse_symbiosis(body: Any) -> list[tuple[str, dict[str, str]]]:
    """Symbiosis: flat list of {symbol, address, chainId, decimals}. Group by symbol."""
    if not isinstance(body, list):
        return []
    groups: dict[str, dict[str, str]] = {}
    for t in body:
        if not isinstance(t, dict):
            continue
        cid = t.get("chainId")
        addr = (t.get("address") or "").lower()
        sym = (t.get("symbol") or "").lower()
        chain = _normalize_chain(cid) if isinstance(cid, int) else None
        if chain and addr.startswith("0x") and len(addr) == 42 and sym:
            groups.setdefault(f"sym-{sym}", {})[chain] = addr
    return [(gid, m) for gid, m in groups.items() if len(m) >= 2]


# ── Exchange listings (CEX-confirmed multi-chain tokens) ─────────────────────
# Reads a file dumped from CEX APIs (Binance/Bybit/Bitget/etc). Format:
#   SYMBOL
#     exchange: NETWORK [contract_addr]
#     ...
# Tokens listed on CEXs are real, tradeable, and have exit liquidity — best
# quality signal source.

_EX_NETWORK_MAP = {
    "BSC":"bsc","BNB":"bsc","BEP20":"bsc",
    "ETH":"ethereum","ETHEREUM":"ethereum","ERC20":"ethereum",
    "ARBITRUM":"arbitrum","ARBEVM":"arbitrum","ARB":"arbitrum",
    "AVAX":"avalanche","CAVAX":"avalanche","AVAC":"avalanche","AVAXC":"avalanche",
    "POLYGON":"polygon","MATIC":"polygon",
    "OPTIMISM":"optimism","OPMAINNET":"optimism","OPETH":"optimism",
    "BASE":"base","SOL":"solana","SOLANA":"solana","TON":"ton",
    "TRON":"tron","TRX":"tron","TRC20":"tron","NEAR":"near",
    "APTOS":"aptos","SUI":"sui","CELO":"celo","KAIA":"kaia","KLAY":"kaia",
    "LINEA":"linea","BLAST":"blast","SCROLL":"scroll",
    "ZKSYNC":"zksync","ZKERA":"zksync",
    "MANTLE":"mantle","MANTLENETWORK":"mantle",
    "BERACHAIN":"berachain","BERA":"berachain",
    "OPBNB":"opbnb","KAVAEVM":"kava","KAVA":"kava",
    "CRONOS":"cronos","CRO":"cronos",
    "MOONBEAM":"moonbeam","METIS":"metis","SONIC":"sonic",
    "HYPEREVM":"hyperliquid","HYPERLIQUID":"hyperliquid",
    "MONAD":"monad","PLUME":"plume","ABSTRACT":"abstract",
    "PULSECHAIN":"pulsechain",
    "MANTA":"manta","MANTANETWORK":"manta",
    "MANTAPACIFIC":"manta","MANTAPACIFICMAINNET":"manta","MANTAETH":"manta",
}


async def refresh_exchange_listings(path: str) -> dict[str, int]:
    """Parse a CEX-listings dump and merge into cg2:group/cg2:contract."""
    import re
    import time as _t
    log.info("bridge.fetching", source="exchanges", url=path)
    t0 = _t.time()

    tokens: dict[str, dict[str, str]] = {}
    cur: str | None = None
    try:
        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                if ln.startswith("#") or not ln.strip():
                    continue
                if not ln.startswith(" "):
                    cur = ln.strip()
                    continue
                m = re.match(r"\s+(\w+):\s*(\w+)\s*(\S+)?", ln)
                if not m or not cur:
                    continue
                _ex, net, addr = m.group(1), m.group(2).upper(), (m.group(3) or "").strip()
                chain = _EX_NETWORK_MAP.get(net)
                if not chain:
                    continue
                if addr.startswith("0x") and len(addr) == 42:
                    tokens.setdefault(cur, {})[chain] = addr.lower()
                elif chain in {"solana","ton","tron","near","aptos","sui"} and addr:
                    tokens.setdefault(cur, {})[chain] = addr
    except FileNotFoundError:
        log.warning("bridge.fetch_failed", source="exchanges",
                    err="file not found")
        return {"contracts": 0}

    log.info("bridge.parsed", source="exchanges",
             groups=len(tokens), sec=round(_t.time() - t0, 1))

    r = await get_redis()
    contracts = multichain = 0
    for sym, mapped in tokens.items():
        if len(mapped) < 2:
            continue   # skip single-chain (we drop them anyway)
        gid = f"exch-{sym.lower()}"
        if await _save_group(r, mapped, "exchanges", gid):
            contracts += len(mapped)
            multichain += 1
    log.info("bridge.saved", source="exchanges",
             contracts=contracts, multichain=multichain)
    return {"contracts": contracts, "multichain": multichain}


# ── Stargate (LayerZero-based, has isBridgeable flag) ────────────────────────

_SG_CHAIN_MAP = {
    "ethereum": "ethereum", "bsc": "bsc", "base": "base", "arbitrum": "arbitrum",
    "avalanche": "avalanche", "polygon": "polygon", "optimism": "optimism",
    "hyperliquid": "hyperliquid", "bera": "berachain", "berachain": "berachain",
    "mantle": "mantle", "sonic": "sonic", "monad": "monad", "linea": "linea",
    "scroll": "scroll", "fantom": "fantom", "blast": "blast", "zksync": "zksync",
    "celo": "celo", "gnosis": "gnosis", "cronos": "cronos", "moonbeam": "moonbeam",
    "core": "core", "plume": "plume", "apechain": "apechain", "mode": "mode",
    "lisk": "lisk", "ink": "ink", "unichain": "unichain", "soneium": "soneium",
    "worldchain": "worldchain", "abstract": "abstract", "sophon": "sophon",
    "pulsechain": "pulsechain", "zora": "zora",
}


async def refresh_stargate() -> dict[str, int]:
    """Stargate Finance — uses isBridgeable flag, much cleaner than Socket."""
    url = "https://stargate.finance/api/tokens"
    return await _fetch_and_save(url, "stargate", _parse_stargate)


def _parse_stargate(body: Any) -> list[tuple[str, dict[str, str]]]:
    """Stargate: list of {chainKey, address, symbol, isBridgeable, ...}.
    Filter isBridgeable=true, group by symbol on supported chains."""
    if not isinstance(body, list):
        return []
    by_sym: dict[str, dict[str, str]] = {}
    for t in body:
        if not isinstance(t, dict) or not t.get("isBridgeable"):
            continue
        sym = (t.get("symbol") or "").lower().strip()
        addr = (t.get("address") or "").lower()
        ckey = (t.get("chainKey") or "").lower()
        chain = _SG_CHAIN_MAP.get(ckey)
        if not chain or len(sym) < 2 or sym.isdigit():
            continue
        if not addr.startswith("0x") or len(addr) != 42:
            continue
        by_sym.setdefault(sym, {})[chain] = addr
    return [(f"sg-{s}", m) for s, m in by_sym.items() if len(m) >= 2]


# ── CCIP (Chainlink Cross-Chain Interoperability Protocol) ────────────────────
# Source: docs.chain.link/ccip/directory/mainnet (Astro SSR, served behind CF).
# Need proxy_manager because Cloudflare blocks direct curl.

_CCIP_INDEX_URL = "https://docs.chain.link/ccip/directory/mainnet"
_CCIP_TOKEN_URL = "https://docs.chain.link/ccip/directory/mainnet/token/{}"

_CCIP_OUR_CHAINS = {
    "ethereum", "bsc", "polygon", "arbitrum", "base", "optimism", "avalanche",
    "fantom", "zksync", "linea", "blast", "scroll", "mantle", "berachain",
    "celo", "cronos", "moonbeam", "gnosis", "worldchain", "unichain",
    "soneium", "mode", "zora", "lisk", "ink", "hyperliquid", "sonic",
    "abstract", "plume", "sophon", "apechain", "core", "monad", "pulsechain",
}


def _ccip_normalize(key: str) -> str | None:
    """CCIP chain key → our internal chain name.
    Examples:
      ethereum-mainnet               -> ethereum
      ethereum-mainnet-arbitrum-1    -> arbitrum
      ethereum-mainnet-zkevm-1       -> polygon (zkevm folded into polygon)
      avalanche-mainnet              -> avalanche
      bsc-mainnet                    -> bsc
    """
    k = key.lower()
    parts = k.split("-")
    if k.startswith("ethereum-mainnet-") and len(parts) >= 3:
        l2 = parts[2]
        if l2 == "zkevm":
            return "polygon"
        return l2 if l2 in _CCIP_OUR_CHAINS else None
    if k.endswith("-mainnet"):
        first = parts[0]
        if first == "ethereum":
            return "ethereum"
        return first if first in _CCIP_OUR_CHAINS else None
    return None


async def _fetch_via_proxy(url: str, pm, attempts: int = 4) -> bytes | None:
    """Fetch through proxy_manager — required for Cloudflare-protected hosts."""
    headers = {"User-Agent": "Mozilla/5.0 Chrome/120.0"}
    timeout = aiohttp.ClientTimeout(total=30)
    for _ in range(attempts):
        proxy = pm.next()
        if not proxy:
            return None
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as s:
                async with s.get(url, proxy=proxy, allow_redirects=True) as r:
                    if r.status == 200:
                        return await r.read()
        except Exception:
            continue
    return None


async def refresh_ccip() -> dict[str, int]:
    """Scrape Chainlink CCIP directory (live deployments).
    Highest-quality source — every token here has a real CCIP TokenPool deployed."""
    import re as _re
    import time
    log.info("bridge.fetching", source="ccip", url=_CCIP_INDEX_URL[:60])
    t0 = time.monotonic()
    pm = get_proxy_manager()

    body = await _fetch_via_proxy(_CCIP_INDEX_URL, pm)
    if not body:
        log.warning("bridge.fetch_failed", source="ccip", err="index unreachable")
        return {"contracts": 0}
    h = body.decode("utf-8", errors="replace").replace("&quot;", '"').replace("&amp;", "&")
    ids = _re.findall(
        r'"id":\[0,"([^"]+)"\],"logo":\[0,"https://d2f70xi62kby8n[^"]+"\],"totalNetworks":\[0,(\d+)\]',
        h,
    )
    log.info("bridge.parsed_index", source="ccip", tokens=len(ids))
    if not ids:
        return {"contracts": 0}

    from urllib.parse import quote
    sem = asyncio.Semaphore(15)
    groups: list[tuple[str, dict[str, str]]] = []

    async def _one(tid: str) -> None:
        url = _CCIP_TOKEN_URL.format(quote(tid))
        async with sem:
            b = await _fetch_via_proxy(url, pm)
        if not b:
            return
        page = b.decode("utf-8", errors="replace").replace("&quot;", '"').replace("&amp;", "&")
        m = _re.search(r'"networks":\[1,\[(.+?)\]\]', page, _re.DOTALL)
        if not m:
            return
        entries = _re.findall(
            r'"key":\[0,"([^"]+)"\][^}]*?"tokenAddress":\[0,"(0x[a-fA-F0-9]{40})"\]',
            m.group(1),
        )
        mapped: dict[str, str] = {}
        for ck, addr in entries:
            chain = _ccip_normalize(ck)
            if chain:
                mapped[chain] = addr.lower()
        if len(mapped) >= 2:
            groups.append((f"ccip-{tid.lower()}", mapped))

    await asyncio.gather(*[_one(tid) for tid, _ in ids])
    log.info("bridge.parsed", source="ccip", groups=len(groups),
             sec=round(time.monotonic() - t0, 1))

    r = await get_redis()
    contracts = multichain = 0
    for gid, mapped in groups:
        if await _save_group(r, mapped, "ccip", gid):
            contracts += len(mapped)
            multichain += 1
    log.info("bridge.saved", source="ccip", contracts=contracts, multichain=multichain)
    return {"contracts": contracts, "multichain": multichain}


# ── Wormhole normalized JSON dump (3344-symbol bulk import) ──────────────────
# Source: pre-normalized export with structure
#   {tokens: {SYMBOL: {decimals, platforms: {chain_key: address}}}}
# Chain keys are CoinGecko-style (polygon-pos, arbitrum-one, optimistic-ethereum,
# binance-smart-chain, …) so we go through _normalize_chain.

async def refresh_wh_normalized(path: str) -> dict[str, int]:
    """One-shot import from wh_wrapped_tokens_normalized.json file."""
    import json
    import time as _t
    log.info("bridge.fetching", source="wh_normalized", url=path)
    t0 = _t.time()

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        log.warning("bridge.fetch_failed", source="wh_normalized",
                    err="file not found")
        return {"contracts": 0}
    except (OSError, json.JSONDecodeError) as e:
        log.warning("bridge.fetch_failed", source="wh_normalized",
                    err=str(e)[:100])
        return {"contracts": 0}

    tokens = data.get("tokens") or {}
    log.info("bridge.parsed", source="wh_normalized",
             groups=len(tokens), sec=round(_t.time() - t0, 1))

    r = await get_redis()
    contracts = multichain = skipped = 0
    for sym, info in tokens.items():
        if not isinstance(info, dict):
            continue
        plats = info.get("platforms") or {}
        mapped: dict[str, str] = {}
        for raw_chain, addr in plats.items():
            chain = _normalize_chain(raw_chain)
            if not chain or not isinstance(addr, str):
                continue
            # Filter Wormhole NTT peer-manager entries — not real token addresses
            if "(peer-manager)" in addr:
                continue
            addr = addr.strip().lower()
            # EVM: 0x + 40 hex
            if addr.startswith("0x"):
                if len(addr) != 42:
                    continue
                mapped[chain] = addr
            # Non-EVM (solana base58, sui/aptos/algorand) — store as-is if non-empty
            elif addr:
                mapped[chain] = addr
        if len(mapped) < 2:
            skipped += 1
            continue
        gid = f"wh-{sym.lower()}"
        if await _save_group(r, mapped, "wormhole", gid):
            contracts += len(mapped)
            multichain += 1

    log.info("bridge.saved", source="wh_normalized",
             contracts=contracts, multichain=multichain, skipped=skipped)
    return {"contracts": contracts, "multichain": multichain, "skipped": skipped}


# ── LayerZero OFT bulk import (lz_oft_tokens.json) ───────────────────────────
# Source: pre-normalized export from LayerZero OFTSent on-chain scan.
# Same shape as wh_wrapped_tokens_normalized.json:
#   {tokens: {SYMBOL: {decimals, platforms: {chain_key: address}}}}

async def refresh_lz_oft(path: str) -> dict[str, int]:
    """One-shot import from lz_oft_tokens.json file."""
    import json
    import time as _t
    log.info("bridge.fetching", source="lz_oft", url=path)
    t0 = _t.time()

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        log.warning("bridge.fetch_failed", source="lz_oft",
                    err="file not found")
        return {"contracts": 0}
    except (OSError, json.JSONDecodeError) as e:
        log.warning("bridge.fetch_failed", source="lz_oft", err=str(e)[:100])
        return {"contracts": 0}

    tokens = data.get("tokens") or {}
    log.info("bridge.parsed", source="lz_oft",
             groups=len(tokens), sec=round(_t.time() - t0, 1))

    r = await get_redis()
    contracts = multichain = skipped = 0
    for sym, info in tokens.items():
        if not isinstance(info, dict):
            continue
        plats = info.get("platforms") or {}
        mapped: dict[str, str] = {}
        for raw_chain, addr in plats.items():
            chain = _normalize_chain(raw_chain)
            if not chain or not isinstance(addr, str):
                continue
            addr = addr.strip().lower()
            if addr.startswith("0x"):
                if len(addr) != 42:
                    continue
                mapped[chain] = addr
            elif addr:
                mapped[chain] = addr
        if len(mapped) < 2:
            skipped += 1
            continue
        gid = f"lz-{sym.lower()}"
        if await _save_group(r, mapped, "layerzero", gid):
            contracts += len(mapped)
            multichain += 1

    log.info("bridge.saved", source="lz_oft",
             contracts=contracts, multichain=multichain, skipped=skipped)
    return {"contracts": contracts, "multichain": multichain, "skipped": skipped}


# ── Common runner ─────────────────────────────────────────────────────────────

async def _fetch_and_save(
    url: str,
    bridge_name: str,
    parser,
    is_json: bool = True,
    headers: dict[str, str] | None = None,
) -> dict[str, int]:
    import time
    log.info("bridge.fetching", source=bridge_name, url=url[:60])
    t0 = time.monotonic()

    proxies = get_proxy_manager()
    proxy = proxies.next()
    kwargs: dict[str, Any] = {"proxy": proxy} if proxy else {}
    if headers:
        kwargs["headers"] = headers
    timeout = aiohttp.ClientTimeout(total=60)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, **kwargs) as resp:
                if resp.status != 200:
                    log.warning("bridge.fetch_failed",
                                source=bridge_name, status=resp.status)
                    return {"contracts": 0}
                if is_json:
                    body = await resp.json(content_type=None)
                else:
                    body = await resp.text()
    except Exception as e:
        log.warning("bridge.fetch_error", source=bridge_name, err=str(e)[:100])
        return {"contracts": 0}

    try:
        groups = parser(body)
    except Exception as e:
        log.warning("bridge.parse_error", source=bridge_name, err=str(e)[:100])
        return {"contracts": 0}

    log.info("bridge.parsed", source=bridge_name, groups=len(groups),
             sec=round(time.monotonic() - t0, 1))

    r = await get_redis()
    contracts = 0
    multichain = 0
    for gid, mapped in groups:
        saved = await _save_group(r, mapped, bridge_name, gid)
        if saved:
            contracts += len(mapped)
            if len(mapped) >= 2:
                multichain += 1

    log.info("bridge.saved", source=bridge_name,
             contracts=contracts, multichain=multichain)
    return {"contracts": contracts, "multichain": multichain}


# ── Top-level API ─────────────────────────────────────────────────────────────

async def refresh_all_bridges() -> dict[str, dict[str, int]]:
    """Refresh all bridge sources in parallel."""
    sources = [
        ("lifi",       refresh_lifi),
        ("axelar",     refresh_axelar),
        ("wormhole",   refresh_wormhole),
        ("stargate",   refresh_stargate),
        ("ccip",       refresh_ccip),
        # Removed: across (+16), symbiosis (+9) — marginal, mostly merged into existing groups
        # Removed: socket — token-lists/all is a junk dump (no coingeckoId), groups by symbol = collisions
    ]
    results = await asyncio.gather(
        *[fn() for _, fn in sources],
        return_exceptions=True,
    )
    out: dict[str, dict[str, int]] = {}
    for (name, _), res in zip(sources, results):
        if isinstance(res, Exception):
            out[name] = {"error": str(res)[:100]}
        else:
            out[name] = res
    return out


async def get_bridges_for(gid: str) -> list[str]:
    """Return list of bridge names that support this token group."""
    r = await get_redis()
    items = await r.smembers(f"cg2:bridges:{gid}")
    return sorted(items)
