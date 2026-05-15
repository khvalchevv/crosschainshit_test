from __future__ import annotations

from config import get_thresholds


def _cfg() -> dict:
    return get_thresholds()["bridge"]


def estimate_bridge_cost(from_chain: str, to_chain: str, trade_usd: float) -> float:
    cfg = _cfg()
    fee_usd = trade_usd * cfg["default_fee_percent"] / 100
    gas_map = cfg["gas_usd"]
    gas_usd = gas_map.get(from_chain, 1.0) + gas_map.get(to_chain, 1.0)
    return fee_usd + gas_usd


def estimate_bridge_cost_pct(from_chain: str, to_chain: str, trade_usd: float) -> float:
    if trade_usd <= 0:
        return 100.0
    return estimate_bridge_cost(from_chain, to_chain, trade_usd) / trade_usd * 100
