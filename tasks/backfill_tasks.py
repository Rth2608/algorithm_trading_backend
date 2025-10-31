from __future__ import annotations

import os
import time
import math
import random
from collections import deque
from typing import Any, Dict, List, Optional, Tuple, Set

from celery import Celery
import httpx

# ──────────────────────────────────────────────────────────────────────────────
# Binance Futures USDT-M 기본값
# ──────────────────────────────────────────────────────────────────────────────
BINANCE_API_BASE = os.environ.get("BINANCE_API_BASE", "https://fapi.binance.com")
BINANCE_KLINES_PATH = os.environ.get("BINANCE_KLINES_PATH", "/fapi/v1/klines")
BINANCE_EXCHANGE_INFO_PATH = os.environ.get(
    "BINANCE_EXCHANGE_INFO_PATH", "/fapi/v1/exchangeInfo"
)

DEFAULT_QUOTE = os.environ.get("DEFAULT_QUOTE", "USDT")
ALLOWED_QUOTES = os.environ.get(
    "ALLOWED_QUOTES", "USDT,USDC,BUSD,FDUSD,TRY,EUR,BRL"
).split(",")

HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "20"))

# ===== (신규/강화) 레이트리미터 & 백오프 파라미터 =====
# 로컬(프로세스) 제한: 과도한 버스트 억제
BINANCE_LOCAL_MAX_RPS = int(os.environ.get("BINANCE_LOCAL_MAX_RPS", "15"))
BINANCE_LOCAL_MAX_RPM = int(os.environ.get("BINANCE_LOCAL_MAX_RPM", "600"))

# 전역(분산, Redis) 제한: 모든 컨테이너/워커 합산
BINANCE_GLOBAL_MAX_RPS = int(os.environ.get("BINANCE_GLOBAL_MAX_RPS", "20"))
BINANCE_GLOBAL_MAX_RPM = int(os.environ.get("BINANCE_GLOBAL_MAX_RPM", "900"))

# Binance 1분 weight 한도(미설정 시 futures 2400 가정)
BINANCE_WEIGHT_LIMIT_1M = int(os.environ.get("BINANCE_WEIGHT_LIMIT_1M", "2400"))
# used-weight가 임계치 이상이면 감속 시작
BINANCE_WEIGHT_SLOWDOWN_RATIO = float(
    os.environ.get("BINANCE_WEIGHT_SLOWDOWN_RATIO", "0.80")
)

BINANCE_MAX_RETRIES = int(os.environ.get("BINANCE_MAX_RETRIES", "6"))
BINANCE_BACKOFF_BASE_SEC = float(os.environ.get("BINANCE_BACKOFF_BASE_SEC", "1.0"))
# 청크 사이 잠깐 쉬기(버스트 완화)
BINANCE_CHUNK_PAUSE_MS = int(os.environ.get("BINANCE_CHUNK_PAUSE_MS", "250"))

# (선택) 개발 편의: 인터벌당 최대 청크 수 제한
MAX_CHUNKS_PER_INTERVAL: Optional[int] = (
    int(os.environ["MAX_CHUNKS_PER_INTERVAL"])
    if "MAX_CHUNKS_PER_INTERVAL" in os.environ
    else None
)

# Redis (전역 레이트리미터 용)
REDIS_URL = os.environ.get(
    "REDIS_URL", os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")
)
REDIS_RATE_KEY_PREFIX = os.environ.get("REDIS_RATE_KEY_PREFIX", "binance:rate")

# Celery
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

celery_app = Celery(
    "backfill_tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND
)
celery_app.conf.update(
    task_track_started=True,
    accept_content=["json"],
    task_serializer="json",
    result_serializer="json",
    result_extended=True,
)

_TASK_SIGNATURE = "v3-dist-rl+weight-aware"

_FAPI_SYMBOLS_CACHE: Optional[Set[str]] = None


# ──────────────────────────────────────────────────────────────────────────────
# 간단한 로컬 레이트리미터(초/분)
# ──────────────────────────────────────────────────────────────────────────────
class _LocalLimiter:
    def __init__(self, max_rps: int, max_rpm: int):
        self.max_rps = max_rps
        self.max_rpm = max_rpm
        self._sec_q = deque()
        self._min_q = deque()

    def acquire(self) -> None:
        now = time.monotonic()
        s_ago = now - 1.0
        m_ago = now - 60.0
        while self._sec_q and self._sec_q[0] <= s_ago:
            self._sec_q.popleft()
        while self._min_q and self._min_q[0] <= m_ago:
            self._min_q.popleft()

        while len(self._sec_q) >= self.max_rps or len(self._min_q) >= self.max_rpm:
            now = time.monotonic()
            s_ago = now - 1.0
            m_ago = now - 60.0
            ws = 0.0 if len(self._sec_q) < self.max_rps else (self._sec_q[0] - s_ago)
            wm = 0.0 if len(self._min_q) < self.max_rpm else (self._min_q[0] - m_ago)
            time.sleep(max(ws, wm) + 0.005)
            while self._sec_q and self._sec_q[0] <= (time.monotonic() - 1.0):
                self._sec_q.popleft()
            while self._min_q and self._min_q[0] <= (time.monotonic() - 60.0):
                self._min_q.popleft()

        t = time.monotonic()
        self._sec_q.append(t)
        self._min_q.append(t)


_LOCAL_RL = _LocalLimiter(BINANCE_LOCAL_MAX_RPS, BINANCE_LOCAL_MAX_RPM)

# ──────────────────────────────────────────────────────────────────────────────
# 분산(전역) 레이트리미터: Redis
# ──────────────────────────────────────────────────────────────────────────────
_redis = None
try:
    import redis  # type: ignore

    _redis = redis.Redis.from_url(
        REDIS_URL, socket_connect_timeout=1.5, socket_timeout=1.5
    )
    # 연결 테스트(실패해도 fallback)
    _redis.ping()
except Exception:
    _redis = None  # Redis 불가 시 로컬만 사용


def _redis_incr_with_ttl(key: str, limit: int, ttl: int) -> bool:
    """
    원자적 INCR. 첫 증가면 TTL 설정. limit 초과하면 롤백(DECR) 후 False.
    """
    if _redis is None:
        return True  # Redis 없으면 패스
    pipe = _redis.pipeline()
    try:
        pipe.incr(key)
        pipe.expire(key, ttl)
        cur, _ = pipe.execute()
        cur = int(cur or 0)
        if cur > limit:
            # 롤백
            try:
                _redis.decr(key)
            except Exception:
                pass
            return False
        return True
    except Exception:
        return True  # 장애 시 과도차단 방지: 통과


def _global_acquire() -> None:
    """
    초/분 전역 제한. 초과 시 다음 경계까지 대기.
    """
    if _redis is None:
        return  # fallback

    while True:
        now = time.time()
        sec_key = f"{REDIS_RATE_KEY_PREFIX}:sec:{int(now)}"
        min_key = f"{REDIS_RATE_KEY_PREFIX}:min:{int(now // 60)}"

        ok_sec = _redis_incr_with_ttl(sec_key, BINANCE_GLOBAL_MAX_RPS, 2)
        ok_min = _redis_incr_with_ttl(min_key, BINANCE_GLOBAL_MAX_RPM, 120)

        if ok_sec and ok_min:
            return

        # 어느 한쪽이라도 막혔으면 다음 경계까지 수면
        now = time.time()
        sleep_sec = 0.0
        if not ok_sec:
            sleep_sec = max(sleep_sec, 1.0 - (now - math.floor(now)))
        if not ok_min:
            # 다음 분 경계까지
            sec_into_min = int(now) % 60
            sleep_sec = max(sleep_sec, 60 - sec_into_min + 0.01)
        time.sleep(min(sleep_sec + 0.01, 1.2))


def normalize_symbol(raw: str) -> str:
    s = raw.upper().strip().replace(" ", "")
    if any(s.endswith(q.upper()) for q in ALLOWED_QUOTES):
        return s
    return f"{s}{DEFAULT_QUOTE.upper()}"


def _update_speed_by_weight(headers: Dict[str, str]) -> None:
    """
    Binance 헤더의 used-weight를 보고 감속. 전역/로컬 RPS를 일시적으로 낮추는 소프트한 방식.
    """
    try:
        used = headers.get("X-MBX-USED-WEIGHT-1M") or headers.get("X-MBX-USED-WEIGHT")
        if not used:
            return
        used = int(str(used).strip())
        if used >= int(BINANCE_WEIGHT_LIMIT_1M * BINANCE_WEIGHT_SLOWDOWN_RATIO):
            # 남은 시간(분 경계까지) 동안 살짝 감속: 청크 대기 증가
            # 0.8 이상이면 400~900ms 사이 랜덤 쉼(혼잡 분산)
            extra_ms = random.randint(400, 900)
            time.sleep(extra_ms / 1000.0)
    except Exception:
        pass


def _load_fapi_symbols(client: httpx.Client) -> Set[str]:
    global _FAPI_SYMBOLS_CACHE
    if _FAPI_SYMBOLS_CACHE is not None:
        return _FAPI_SYMBOLS_CACHE
    url = f"{BINANCE_API_BASE}{BINANCE_EXCHANGE_INFO_PATH}"

    # 전역 + 로컬 제한
    _global_acquire()
    _LOCAL_RL.acquire()
    resp = client.get(url)
    if resp.status_code == 429:
        ra = resp.headers.get("Retry-After")
        time.sleep(float(ra) if ra else 2.0)
        _global_acquire()
        _LOCAL_RL.acquire()
        resp = client.get(url)

    resp.raise_for_status()
    data = resp.json()
    syms: Set[str] = set()
    for item in data.get("symbols", []):
        sym = str(item.get("symbol", "")).upper()
        status = str(item.get("status", "")).upper()
        if sym and status == "TRADING":
            syms.add(sym)
    _FAPI_SYMBOLS_CACHE = syms
    return syms


def _binance_get_with_retry(
    client: httpx.Client, url: str, params: Dict[str, Any]
) -> httpx.Response:
    """
    - (전역) Redis + (로컬) 큐 동시 사용
    - 429/5xx: Retry-After 우선, 없으면 지수 백오프 + 지터
    - used-weight 기반 일시 감속
    """
    last_resp: Optional[httpx.Response] = None
    for attempt in range(BINANCE_MAX_RETRIES + 1):
        _global_acquire()
        _LOCAL_RL.acquire()
        resp = client.get(url, params=params)

        # 헤더 기반 가변 슬로틀
        _update_speed_by_weight(resp.headers)

        # 429면 백오프
        if resp.status_code == 429 or "Too many requests" in resp.text:
            ra = resp.headers.get("Retry-After")
            if ra:
                sleep_for = float(ra)
            else:
                sleep_for = (
                    BINANCE_BACKOFF_BASE_SEC * (2**attempt)
                ) + random.uniform(0, 0.35)
            time.sleep(min(sleep_for, 60.0))
            last_resp = resp
            continue

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError:
            # 5xx → 백오프 재시도
            if 500 <= resp.status_code < 600 and attempt < BINANCE_MAX_RETRIES:
                sleep_for = (
                    BINANCE_BACKOFF_BASE_SEC * (2**attempt)
                ) + random.uniform(0, 0.35)
                time.sleep(min(sleep_for, 30.0))
                last_resp = resp
                continue
            raise
        return resp

    # 재시도 초과
    sc = getattr(last_resp, "status_code", "N/A")
    txt = last_resp.text[:300] if last_resp else ""
    raise ValueError(
        f"Klines HTTP {sc} (max retries exceeded) url='{url}', params={params}, text='{txt}'"
    )


def _fetch_all_klines_via_rest(
    *,
    client: httpx.Client,
    symbol: str,
    interval: str,
    start_ms: int = 0,
    end_ms: Optional[int] = None,
) -> Tuple[int, Optional[int]]:
    url = f"{BINANCE_API_BASE}{BINANCE_KLINES_PATH}"
    total = 0
    last_open: Optional[int] = None
    chunks = 0

    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": 1000,
            "startTime": start_ms,
        }
        if end_ms is not None:
            params["endTime"] = end_ms

        resp = _binance_get_with_retry(client, url, params)
        data = resp.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected response for {symbol}-{interval}: {data}")
        if not data:
            break

        total += len(data)
        last_open = int(data[-1][0])
        start_ms = last_open + 1
        chunks += 1

        if MAX_CHUNKS_PER_INTERVAL is not None and chunks >= MAX_CHUNKS_PER_INTERVAL:
            break
        if len(data) < 1000:
            break

        # 청크 사이 버스트 완화 + 소량 지터
        base_ms = BINANCE_CHUNK_PAUSE_MS
        jitter_ms = random.randint(50, 150)
        time.sleep((base_ms + jitter_ms) / 1000.0)

    return total, last_open


def _run_backfill_for_symbol_interval(
    client: httpx.Client, symbol: str, interval: str
) -> Dict[str, Any]:
    fetched, last_open = _fetch_all_klines_via_rest(
        client=client, symbol=symbol, interval=interval, start_ms=0, end_ms=None
    )
    return {
        "symbol": symbol,
        "interval": interval,
        "rows_fetched": fetched,
        "last_open_ms": last_open,
        "source": f"{BINANCE_API_BASE}{BINANCE_KLINES_PATH}",
        "ok": True,
    }


def _progress(
    self,
    *,
    status: str,
    current: int,
    total: int,
    symbol: str = "N/A",
    interval: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    meta: Dict[str, Any] = {
        "status": f"[{_TASK_SIGNATURE}] {status}",
        "current": int(current),
        "total": int(total),
        "interval_percentage": float(0 if total == 0 else (current / total) * 100.0),
        "symbol": symbol,
    }
    if interval is not None:
        meta["interval"] = interval
    if extra:
        meta.update(extra)
    self.update_state(state="PROGRESS", meta=meta)


@celery_app.task(
    bind=True, name="tasks.backfill_tasks.backfill_symbol_all_intervals_task"
)
def backfill_symbol_all_intervals_task(
    self, symbol: str, intervals: Optional[List[str]] = None, **kwargs
) -> Dict[str, Any]:
    try:
        if intervals is None:
            intervals = kwargs.get("intervals")
        if intervals is None:
            intervals = ["1m", "5m", "15m", "1h", "4h", "1d"]

        normalized = normalize_symbol(symbol)

        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            tradables = _load_fapi_symbols(client)
            if normalized not in tradables:
                raise ValueError(
                    f"[{_TASK_SIGNATURE}] Not tradable on fapi: raw='{symbol}', normalized='{normalized}'."
                )

            total = len(intervals)
            _progress(
                self,
                status="시작",
                current=0,
                total=total,
                symbol=normalized,
                extra={"raw_symbol": symbol},
            )

            results: List[Dict[str, Any]] = []
            for idx, interval in enumerate(intervals, start=1):
                _progress(
                    self,
                    status=f"{normalized} - {interval} 수집 중",
                    current=idx - 1,
                    total=total,
                    symbol=normalized,
                    interval=interval,
                )

                # 인터벌 사이에도 아주 소량 지터(동시 폭주 분산)
                time.sleep(random.uniform(0.05, 0.20))

                res = _run_backfill_for_symbol_interval(client, normalized, interval)
                results.append(res)

                _progress(
                    self,
                    status=f"{normalized} - {interval} 완료",
                    current=idx,
                    total=total,
                    symbol=normalized,
                    interval=interval,
                )

        return {
            "task_signature": _TASK_SIGNATURE,
            "symbol": normalized,
            "total_intervals": total,
            "results": results,
            "status": "done",
        }
    except Exception as e:
        raise e
