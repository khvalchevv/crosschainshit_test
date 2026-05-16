"""Address normalization.

EVM addresses are hex and case-insensitive — they arrive in different
casings from different sources (CoinGecko lowercase, DexScreener checksummed)
so they MUST be lowercased to a single canonical form, or write/read keys
won't match.

Non-EVM addresses (Solana / Tron / TON base58, etc.) are case-SENSITIVE —
lowercasing them produces a different, non-existent address. Those are kept
verbatim.

Rule: a 0x-prefixed address is EVM-style hex (also covers Aptos/Sui 32-byte
hex, where lowercasing is harmless) → lowercase. Anything else → keep as-is.
This depends only on the address string, so it's automatically consistent
between the code path that writes a Redis key and the one that reads it.
"""
from __future__ import annotations


def norm_addr(addr: str | None) -> str:
    if not addr:
        return ""
    a = addr.strip()
    if a[:2].lower() == "0x":
        return a.lower()
    return a
