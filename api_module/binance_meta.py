from __future__ import annotations
import time
from typing import Set
from api_module.http_client import BinanceHttpClient


def load_fapi_symbols(client: BinanceHttpClient, future_info_url: str) -> Set[str]:
    """
    Binance Futures 교환정보에서 TRADING 심볼만 골라 Set으로 반환.
    429면 Retry-After 준수하며 한 번 재시도.
    """
    resp = client.get_once(future_info_url, params={})

    data = resp.json()
    syms: Set[str] = set()
    for item in data.get("symbols", []):
        sym = str(item.get("symbol", "")).upper()
        status = str(item.get("status", "")).upper()
        if sym and status == "TRADING":
            syms.add(sym)
    return syms
