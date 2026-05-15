"""Quick smoke test: each new price client returns real data."""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from core.defillama import DefiLlamaClient
from core.geckoterminal import GeckoTerminalClient
from core.jupiter import JupiterClient
from core.kyberswap import KyberSwapClient
from core.okx import OKXClient


async def main() -> None:
    # Known good test tokens
    USDT_ETH = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    WETH_ETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_BSC = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
    SOL_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    queries = [
        ("ethereum", USDT_ETH),
        ("ethereum", WETH_ETH),
        ("bsc",      USDC_BSC),
    ]

    # ── DefiLlama ────────────────────────────────────────────────────────
    print("=== DefiLlama ===")
    dl = DefiLlamaClient()
    t0 = time.monotonic()
    res = await dl.fetch_prices(queries)
    print(f"  {len(res)}/{len(queries)} resolved in {time.monotonic()-t0:.2f}s")
    for k, v in res.items():
        print(f"    {k[0]:<10} {k[1][:10]}…  ${v:.4f}")
    await dl.close()

    # ── GeckoTerminal ────────────────────────────────────────────────────
    print("\n=== GeckoTerminal ===")
    gt = GeckoTerminalClient()
    t0 = time.monotonic()
    res = await gt.fetch_prices(queries)
    print(f"  {len(res)}/{len(queries)} resolved in {time.monotonic()-t0:.2f}s")
    for k, v in res.items():
        print(f"    {k[0]:<10} {k[1][:10]}…  ${v:.4f}")
    await gt.close()

    # ── OKX ──────────────────────────────────────────────────────────────
    print("\n=== OKX ===")
    okx = OKXClient()
    t0 = time.monotonic()
    res = await okx.fetch_prices(queries)
    print(f"  {len(res)}/{len(queries)} resolved in {time.monotonic()-t0:.2f}s")
    for k, info in res.items():
        print(f"    {k[0]:<10} {k[1][:10]}…  ${info['price']:.4f}  liq=${info['liq']:,.0f}")
    await okx.close()

    # ── Jupiter ──────────────────────────────────────────────────────────
    print("\n=== Jupiter (Solana) ===")
    jup = JupiterClient()
    t0 = time.monotonic()
    res = await jup.fetch_prices([SOL_USDC])
    print(f"  {len(res)}/1 resolved in {time.monotonic()-t0:.2f}s")
    for mint, info in res.items():
        print(f"    {mint[:14]}…  ${info['price']:.6f}  liq=${info['liq']:,.0f}")
    await jup.close()

    # ── KyberSwap ────────────────────────────────────────────────────────
    print("\n=== KyberSwap (aggregator quote) ===")
    ks = KyberSwapClient()
    for chain, addr in [("ethereum", WETH_ETH), ("bsc", USDC_BSC)]:
        t0 = time.monotonic()
        q = await ks.quote(chain, addr)
        if q:
            print(f"    {chain:<10} {addr[:10]}…  in=${q['in_usd']:,.2f}  out=${q['out_usd']:,.2f}  slip={q['slippage_pct']:.2f}%  ({time.monotonic()-t0:.2f}s)")
        else:
            print(f"    {chain:<10} {addr[:10]}…  NO ROUTE  ({time.monotonic()-t0:.2f}s)")
    await ks.close()


if __name__ == "__main__":
    asyncio.run(main())
