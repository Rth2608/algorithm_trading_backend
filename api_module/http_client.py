import random
import time
from typing import Any, Dict, Optional
import httpx

from api_module.redis_limiter import RedisWindowLimiter
from api_module.weight_pacer import (
    update_speed_by_weight,
    pace_next_request_by_used_weight,
)


class BinanceHttpClient:
    def __init__(
        self,
        timeout: float,
        limiter: Optional[RedisWindowLimiter],
        weight_limit_1m: int,
        slowdown_ratio: float,
        backoff_base_sec: float,
        max_retries: int,
    ):
        self._client = httpx.Client(timeout=timeout)
        self._limiter = limiter
        self._weight_limit_1m = weight_limit_1m
        self._slowdown_ratio = slowdown_ratio
        self._backoff_base_sec = backoff_base_sec
        self._max_retries = max_retries

    def close(self):
        self._client.close()

    def __enter__(self) -> "BinanceHttpClient":
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def get_with_retry(self, url: str, params: Dict[str, Any]) -> httpx.Response:
        last_resp: Optional[httpx.Response] = None
        for attempt in range(self._max_retries + 1):
            if self._limiter:
                self._limiter.acquire()

            resp = self._client.get(url, params=params)

            update_speed_by_weight(
                resp.headers, self._weight_limit_1m, self._slowdown_ratio
            )
            pace_next_request_by_used_weight(resp.headers, self._weight_limit_1m)

            if resp.status_code == 429 or "Too many requests" in resp.text:
                ra = resp.headers.get("Retry-After")
                sleep_for = (
                    float(ra)
                    if ra
                    else (
                        self._backoff_base_sec * (2**attempt) + random.uniform(0, 0.35)
                    )
                )
                time.sleep(min(sleep_for, 60.0))
                last_resp = resp
                continue

            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError:
                if 500 <= resp.status_code < 600 and attempt < self._max_retries:
                    sleep_for = self._backoff_base_sec * (
                        2**attempt
                    ) + random.uniform(0, 0.35)
                    time.sleep(min(sleep_for, 30.0))
                    last_resp = resp
                    continue
                raise
            return resp

        sc = getattr(last_resp, "status_code", "N/A")
        txt = last_resp.text[:300] if last_resp else ""
        raise ValueError(
            f"HTTP {sc} (max retries exceeded) url='{url}', params={params}, text='{txt}'"
        )

    def get_once(self, url: str, params: Dict[str, Any]) -> httpx.Response:
        if self._limiter:
            self._limiter.acquire()
        resp = self._client.get(url, params=params)
        update_speed_by_weight(
            resp.headers, self._weight_limit_1m, self._slowdown_ratio
        )
        pace_next_request_by_used_weight(resp.headers, self._weight_limit_1m)
        resp.raise_for_status()
        return resp
