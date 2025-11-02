import os
import time
from typing import Any, Dict, List, Optional, Tuple, Callable
from datetime import datetime, timezone
from loguru import logger

from celery import Celery
from sqlalchemy.orm import Session as SyncSession
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

import redis

from db_module.connect_sqlalchemy_engine import SyncSessionLocal
from api_module.weight_pacer import (
    update_speed_by_weight,
    pace_next_request_by_used_weight,
)
from models.ohlcv_data import Ohlcv1m, Ohlcv5m, Ohlcv15m, Ohlcv1h, Ohlcv4h, Ohlcv1d
from api_module.redis_limiter import RedisWindowLimiter
from api_module.http_client import BinanceHttpClient
from api_module.binance_meta import load_fapi_symbols

_MS_PER_INTERVAL: Dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}
MODEL_MAP = {
    "1m": Ohlcv1m,
    "5m": Ohlcv5m,
    "15m": Ohlcv15m,
    "1h": Ohlcv1h,
    "4h": Ohlcv4h,
    "1d": Ohlcv1d,
}
_TASK_SIGNATURE = "Rest Api"

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
BINANCE_API_BASE = os.getenv("BINANCE_API_BASE", "https://fapi.binance.com")
BINANCE_REST_KLINES_PATH = os.getenv("BINANCE_REST_KLINES_PATH", "/fapi/v1/klines")
BINANCE_EXCHANGE_INFO_PATH = os.getenv(
    "BINANCE_EXCHANGE_INFO_PATH", "/fapi/v1/exchangeInfo"
)
BINANCE_SERVER_TIME_PATH = "/fapi/v1/time"

RESTAPI_URL = f"{BINANCE_API_BASE}{BINANCE_REST_KLINES_PATH}"
EXCHANGE_INFO_URL = f"{BINANCE_API_BASE}{BINANCE_EXCHANGE_INFO_PATH}"
SERVER_TIME_URL = f"{BINANCE_API_BASE}{BINANCE_SERVER_TIME_PATH}"

BINANCE_GLOBAL_MAX_RPS = int(os.getenv("BINANCE_GLOBAL_MAX_RPS", "8"))
BINANCE_GLOBAL_MAX_RPM = int(os.getenv("BINANCE_GLOBAL_MAX_RPM", "400"))
BINANCE_WEIGHT_LIMIT_1M = int(os.getenv("BINANCE_WEIGHT_LIMIT_1M", "2400"))
BINANCE_WEIGHT_SLOWDOWN_RATIO = float(os.getenv("BINANCE_WEIGHT_SLOWDOWN_RATIO", "0.8"))
BINANCE_MAX_RETRIES = int(os.getenv("BINANCE_MAX_RETRIES", "4"))
BINANCE_BACKOFF_BASE_SEC = float(os.getenv("BINANCE_BACKOFF_BASE_SEC", "0.5"))

REDIS_URL = os.environ.get("REDIS_URL")
REDIS_RATE_KEY_PREFIX = os.environ.get("REDIS_RATE_KEY_PREFIX", "binance:rate")

# 진행 중 작업을 보관할 키(해시: symbol -> job_id), TTL은 키에 걸지 않고 heartbeat로 주기 갱신
ACTIVE_HASH_KEY = os.environ.get("ACTIVE_HASH_KEY", "ohlcv:active_jobs")
# 각 종에 별도 TTL을 줄 수 없으니, 보조로 set을 두고 마지막 하트비트 epoch(ms)도 저장
ACTIVE_TS_HASH_KEY = os.environ.get("ACTIVE_TS_HASH_KEY", "ohlcv:active_jobs_ts")
ACTIVE_STALE_MS = int(
    os.environ.get("ACTIVE_STALE_MS", str(10 * 60 * 1000))
)  # 10분간 하트비트 없으면 stale

_redis: Optional[redis.Redis] = (
    redis.Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None
)

CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL")
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND")

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


def _symbol_api_to_db(symbol: str) -> str:
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def _fmt_iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _last_closed_open_ms(server_ms: int, step_ms: int) -> int:
    current_candle_open_ms = (server_ms // step_ms) * step_ms
    return current_candle_open_ms - step_ms


def _percent_by_time(
    start_ms_base: int, latest_open_ms: int, server_ms_snapshot: int, step_ms: int
) -> float:
    last_closed = _last_closed_open_ms(server_ms_snapshot, step_ms)
    if last_closed <= start_ms_base:
        return 100.0
    done = max(0, latest_open_ms - start_ms_base)
    total = last_closed - start_ms_base
    return max(0.0, min(100.0, (done / total) * 100.0))


def _get_server_time_ms(http: BinanceHttpClient) -> int:
    r = http.get_once(SERVER_TIME_URL, params=None)
    j = r.json()
    return int(j["serverTime"])


def _get_start_ms_from_db(db: SyncSession, api_symbol: str, interval: str) -> int:
    OhlcvModel = MODEL_MAP[interval]
    symbol = _symbol_api_to_db(api_symbol)
    last_timestamp = db.execute(
        select(func.max(OhlcvModel.timestamp)).where(OhlcvModel.symbol == symbol)
    ).scalar_one_or_none()
    if last_timestamp is None:
        return 0
    return int(last_timestamp.timestamp() * 1000)


def _rows_from_klines(api_symbol: str, data: List[list]) -> List[Dict[str, Any]]:
    base_symbol = _symbol_api_to_db(api_symbol)
    rows: List[Dict[str, Any]] = []
    for k in data:
        open_ms = int(k[0])
        rows.append(
            {
                "symbol": base_symbol,
                "timestamp": datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            }
        )
    return rows


def _bulk_upsert(session: SyncSession, model, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    stmt = pg_insert(model).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=["symbol", "timestamp"])
    result = session.execute(stmt)
    session.commit()
    return result.rowcount or 0


def _active_heartbeat(symbol: str, job_id: str):
    if not _redis:
        return
    now_ms = int(time.time() * 1000)
    _redis.hset(ACTIVE_HASH_KEY, symbol, job_id)
    _redis.hset(ACTIVE_TS_HASH_KEY, symbol, str(now_ms))


def _active_clear(symbol: str, job_id: str):
    if not _redis:
        return
    try:
        cur = _redis.hget(ACTIVE_HASH_KEY, symbol)
        if cur == job_id:
            _redis.hdel(ACTIVE_HASH_KEY, symbol)
            _redis.hdel(ACTIVE_TS_HASH_KEY, symbol)
    except Exception:
        pass


def _fetch_all_klines_restapi(
    *,
    http: BinanceHttpClient,
    db: SyncSession,
    api_symbol: str,
    interval: str,
    start_ms: int = 0,
    end_ms: Optional[int] = None,
    progress_cb: Optional[Callable[[float, int, int], None]] = None,
) -> Tuple[int, Optional[int]]:
    total = 0
    last_open: Optional[int] = None
    start_ms_base = start_ms
    step_ms = _MS_PER_INTERVAL[interval]
    OhlcvModel = MODEL_MAP[interval]

    while True:
        server_ms_snapshot = int(time.time() * 1000)
        params: Dict[str, Any] = {
            "symbol": api_symbol,
            "interval": interval,
            "limit": 1000,
            "startTime": start_ms,
        }
        if end_ms is not None:
            params["endTime"] = end_ms

        resp = http.get_with_retry(RESTAPI_URL, params)
        data = resp.json()
        if not isinstance(data, list):
            raise RuntimeError(
                f"Unexpected response for {api_symbol}-{interval}: {data}"
            )
        if not data:
            break

        rows = _rows_from_klines(api_symbol, data)
        _ = _bulk_upsert(db, OhlcvModel, rows)

        total += len(data)
        last_open = int(data[-1][0])
        start_ms = last_open + 1

        last_updated_ms = int(time.time() * 1000)
        pct = _percent_by_time(start_ms_base, last_open, server_ms_snapshot, step_ms)
        if progress_cb:
            try:
                progress_cb(pct, last_updated_ms, last_open)
            except Exception as e:
                logger.debug("progress_cb error: {}", e)

        if len(data) < 1000:
            break

        headers: Dict[str, str] = dict(resp.headers)
        update_speed_by_weight(
            headers, BINANCE_WEIGHT_LIMIT_1M, BINANCE_WEIGHT_SLOWDOWN_RATIO
        )
        pace_next_request_by_used_weight(headers, BINANCE_WEIGHT_LIMIT_1M)

    return total, last_open


def _progress(
    self,
    *,
    status: str,
    current: int,
    total: int,
    symbol: str = "N/A",
    interval: Optional[str] = None,
    pct_time: Optional[float] = None,
    last_updated_ms: Optional[int] = None,
    latest_open_ms: Optional[int] = None,
) -> None:
    meta: Dict[str, Any] = {
        "status": f"[{_TASK_SIGNATURE}] {status}",
        "current": int(current),
        "total": int(total),
        "pct_intervals": float(0 if total == 0 else (current / total) * 100.0),
        "symbol": symbol,
    }
    if interval is not None:
        meta["interval"] = interval
    if pct_time is not None:
        meta["pct_time"] = float(pct_time)
    if last_updated_ms is not None:
        meta["last_updated_ms"] = int(last_updated_ms)
        meta["last_updated_iso"] = _fmt_iso_utc(last_updated_ms)
    if latest_open_ms is not None:
        meta["latest_open_ms"] = int(latest_open_ms)
        meta["latest_open_iso"] = _fmt_iso_utc(latest_open_ms)

    self.update_state(state="PROGRESS", meta=meta)


def _run_backfill_for_symbol_interval(
    http: BinanceHttpClient,
    db: SyncSession,
    symbol: str,
    interval: str,
    progress_hook,
    start_ms: int,
) -> Dict[str, Any]:
    def _time_cb(pct_time: float, last_updated_ms: int, latest_open_ms: int):
        progress_hook(pct_time, last_updated_ms, latest_open_ms)

    fetched, last_open = _fetch_all_klines_restapi(
        http=http,
        db=db,
        api_symbol=symbol,
        interval=interval,
        start_ms=start_ms,
        end_ms=None,
        progress_cb=_time_cb,
    )
    return {
        "symbol": symbol,
        "interval": interval,
        "rows_fetched": fetched,
        "last_open_ms": last_open,
        "source": RESTAPI_URL,
        "ok": True,
    }


@celery_app.task(bind=True, name="backfill_all")
def backfill_all(
    self, api_symbol: str, intervals: Optional[List[str]] = None
) -> Dict[str, Any]:
    if intervals is None:
        intervals = ["1m", "5m", "15m", "1h", "4h", "1d"]

    limiter = RedisWindowLimiter(
        redis_url=REDIS_URL,
        prefix=REDIS_RATE_KEY_PREFIX,
        max_rps=BINANCE_GLOBAL_MAX_RPS,
        max_rpm=BINANCE_GLOBAL_MAX_RPM,
    )

    base_symbol = _symbol_api_to_db(api_symbol)
    job_id = self.request.id or ""

    _active_heartbeat(base_symbol, job_id)

    try:
        with BinanceHttpClient(
            timeout=HTTP_TIMEOUT,
            limiter=limiter,
            weight_limit_1m=BINANCE_WEIGHT_LIMIT_1M,
            slowdown_ratio=BINANCE_WEIGHT_SLOWDOWN_RATIO,
            backoff_base_sec=BINANCE_BACKOFF_BASE_SEC,
            max_retries=BINANCE_MAX_RETRIES,
        ) as http:

            tradables = load_fapi_symbols(http, EXCHANGE_INFO_URL)
            if api_symbol not in tradables:
                raise ValueError(
                    f"[{_TASK_SIGNATURE}] '{api_symbol}'는 Binance Future에서 ohlcv 데이터를 제공하지 않습니다."
                )

            results: List[Dict[str, Any]] = []

            with SyncSessionLocal() as db:
                total = len(intervals)
                _progress(
                    self,
                    status="Starting",
                    current=0,
                    total=total,
                    symbol=api_symbol,
                    pct_time=0.0,
                )
                _active_heartbeat(base_symbol, job_id)

                for idx, interval in enumerate(intervals, start=1):
                    start_ms = _get_start_ms_from_db(db, api_symbol, interval)

                    _progress(
                        self,
                        status=f"{api_symbol} - {interval} 수집 시작",
                        current=idx - 1,
                        total=total,
                        symbol=api_symbol,
                        interval=interval,
                        pct_time=0.0,
                    )
                    _active_heartbeat(base_symbol, job_id)

                    def _hook(
                        pct_time: float, last_updated_ms: int, latest_open_ms: int
                    ):
                        _progress(
                            self,
                            status=f"{api_symbol} - {interval} 수집 중",
                            current=idx - 1,
                            total=total,
                            symbol=api_symbol,
                            interval=interval,
                            pct_time=pct_time,
                            last_updated_ms=last_updated_ms,
                            latest_open_ms=latest_open_ms,
                        )
                        _active_heartbeat(base_symbol, job_id)

                    res = _run_backfill_for_symbol_interval(
                        http=http,
                        db=db,
                        symbol=api_symbol,
                        interval=interval,
                        progress_hook=_hook,
                        start_ms=start_ms,
                    )
                    results.append(res)

                    latest_open_ms = res.get("last_open_ms") or 0
                    now_ms = int(time.time() * 1000)

                    _progress(
                        self,
                        status=f"{api_symbol} - {interval} 완료",
                        current=idx,
                        total=total,
                        symbol=api_symbol,
                        interval=interval,
                        pct_time=100.0,
                        last_updated_ms=now_ms,
                        latest_open_ms=latest_open_ms,
                    )
                    _active_heartbeat(base_symbol, job_id)

        return {
            "task_signature": _TASK_SIGNATURE,
            "symbol": api_symbol,
            "total_intervals": len(intervals),
            "results": results,
            "status": "done",
        }
    finally:
        _active_clear(base_symbol, job_id)
