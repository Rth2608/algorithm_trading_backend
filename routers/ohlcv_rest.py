from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from celery.result import AsyncResult

from db_module.connect_sqlalchemy_engine import get_async_db
from models import CryptoInfo

from tasks.backfill_tasks import celery_app
from tasks.backfill_tasks import backfill_all

import os
from redis.asyncio import from_url as redis_from_url

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
ACTIVE_HASH_KEY = os.getenv("ACTIVE_HASH_KEY", "ohlcv:active_jobs")

router = APIRouter(prefix="/ohlcv/rest", tags=["OHLCV-REST"])

_redis = redis_from_url(REDIS_URL, encoding="utf-8", decode_responses=True)


async def _active_put(symbol: str, job_id: str) -> None:
    await _redis.hset(ACTIVE_HASH_KEY, symbol, job_id)


async def _active_remove(symbol: str) -> None:
    await _redis.hdel(ACTIVE_HASH_KEY, symbol)


async def _active_all() -> Dict[str, str]:
    data = await _redis.hgetall(ACTIVE_HASH_KEY)
    return data or {}


def _default_intervals(intervals: Optional[List[str]]) -> List[str]:
    return intervals or ["1m", "5m", "15m", "1h", "4h", "1d"]


async def _load_all_symbols(db: AsyncSession) -> List[str]:
    rows = (await db.execute(select(CryptoInfo.symbol))).scalars().all()
    return sorted(set(map(str, rows)))


def _serialize_task(ar: AsyncResult) -> Dict[str, Any]:
    try:
        state = ar.state or "PENDING"
    except Exception:
        state = "PENDING"

    payload: Dict[str, Any] = {
        "state": state,
        "status": "",
        "current": 0,
        "total": 0,
        "pct_intervals": 0.0,
        "symbol": None,
        "interval": None,
        "pct_time": 0.0,
        "last_updated_ms": None,
        "last_updated_iso": None,
        "latest_open_ms": None,
        "latest_open_iso": None,
        "error_info": None,
        "result": None,
    }

    if state == "SUCCESS":
        try:
            payload["result"] = ar.result
        except Exception:
            payload["result"] = None

    try:
        meta = ar.info
    except Exception:
        meta = None

    if isinstance(meta, dict):
        payload["status"] = str(meta.get("status") or "")
        for k, cast, dst in [
            ("current", int, "current"),
            ("total", int, "total"),
            ("pct_intervals", float, "pct_intervals"),
            ("pct_time", float, "pct_time"),
        ]:
            v = meta.get(k)
            if v is not None:
                try:
                    payload[dst] = cast(v)
                except Exception:
                    pass

        payload["symbol"] = meta.get("symbol")
        payload["interval"] = meta.get("interval")

        lu_ms = meta.get("last_updated_ms")
        if lu_ms is not None:
            try:
                payload["last_updated_ms"] = int(lu_ms)
            except Exception:
                pass
        payload["last_updated_iso"] = meta.get("last_updated_iso")

        lo_ms = meta.get("latest_open_ms")
        if lo_ms is not None:
            try:
                payload["latest_open_ms"] = int(lo_ms)
            except Exception:
                pass
        payload["latest_open_iso"] = meta.get("latest_open_iso")

        if state == "FAILURE":
            payload["error_info"] = (
                meta.get("exc_message") or meta.get("error") or str(meta)
            )
    else:
        if state == "FAILURE":
            try:
                payload["error_info"] = (
                    str(meta) if meta is not None else "Unknown error"
                )
            except Exception:
                payload["error_info"] = "Unknown error"

    return payload


async def _prune_finished_active() -> Dict[str, str]:
    """
    Redis의 진행중 목록에서 완료(SUCCESS/FAILURE/REVOKED)된 것 제거
    남은 {symbol: job_id}를 반환
    """
    mapping = await _active_all()
    if not mapping:
        return {}

    to_delete: List[str] = []
    for symbol, jid in mapping.items():
        try:
            ar = celery_app.AsyncResult(jid)
            st = ar.state or "PENDING"
        except Exception:
            st = "PENDING"
        if st in ("SUCCESS", "FAILURE", "REVOKED"):
            to_delete.append(symbol)

    if to_delete:
        await _redis.hdel(ACTIVE_HASH_KEY, *to_delete)

    return await _active_all()


@router.post("/backfill/all-symbols")
async def trigger_backfill_all_symbols(
    intervals: Optional[List[str]] = Body(default=None, embed=True),
    db: AsyncSession = Depends(get_async_db),
):
    """
    metadata.crypto_info 내 모든 심볼 대상 백필 큐잉
    (각 종목은 'USDT'를 붙여 Binance로 호출)
    """
    symbols = await _load_all_symbols(db)
    if not symbols:
        raise HTTPException(status_code=404, detail="표시할 종목 정보가 없습니다.")

    using_intervals = _default_intervals(intervals)

    job_ids: List[str] = []
    symbol_job_map: Dict[str, str] = {}

    for raw in symbols:
        norm_api_symbol = raw.upper().strip().replace(" ", "") + "USDT"
        ar = backfill_all.apply_async(
            kwargs={"api_symbol": norm_api_symbol, "intervals": using_intervals}
        )
        job_ids.append(ar.id)
        symbol_job_map[raw] = ar.id
        await _active_put(raw, ar.id)

    return {
        "message": "queued",
        "symbols_count": len(symbols),
        "intervals": using_intervals,
        "job_ids": job_ids,
        "symbol_job_map": symbol_job_map,
    }


@router.post("/backfill/symbol")
async def trigger_backfill_symbol(
    symbol: str = Body(..., embed=True),
    intervals: Optional[List[str]] = Body(default=None, embed=True),
):
    """
    단일 심볼 백필 큐잉
    """
    use_intervals = _default_intervals(intervals)
    norm_api_symbol = symbol.upper().strip().replace(" ", "") + "USDT"

    ar = backfill_all.apply_async(
        kwargs={"api_symbol": norm_api_symbol, "intervals": use_intervals}
    )
    await _active_put(symbol.upper().strip(), ar.id)

    return {
        "message": "queued",
        "symbol_input": symbol,
        "symbol_used": norm_api_symbol,
        "intervals": use_intervals,
        "job_id": ar.id,
    }


@router.get("/status/active")
async def get_active_jobs_status():
    """
    새로고침 시 진행 중 작업을 복구하기 위한 엔드포인트
    Redis에서 {symbol: job_id}를 가져와 상태를 직렬화
    완료된 job은 즉시 정리 후 반환
    """
    mapping = await _prune_finished_active()
    if not mapping:
        return {"job_ids": [], "symbol_job_map": {}, "items": []}

    items: List[Dict[str, Any]] = []
    for sym, jid in mapping.items():
        ar = celery_app.AsyncResult(jid)
        payload = _serialize_task(ar)
        if not payload.get("symbol"):
            payload["symbol"] = sym
        items.append({"job_id": jid, **payload})

    return {
        "job_ids": list(mapping.values()),
        "symbol_job_map": mapping,
        "items": items,
    }


@router.post("/status/bulk")
async def get_task_status_bulk(job_ids: List[str] = Body(..., embed=True)):
    """
    여러 job_id를 한 번에 조회
    """
    if not job_ids:
        raise HTTPException(
            status_code=422, detail="조회할 작업이 선택되지 않았습니다."
        )

    items: List[Dict[str, Any]] = []
    for jid in job_ids:
        ar = celery_app.AsyncResult(jid)
        payload = _serialize_task(ar)
        items.append({"job_id": jid, **payload})

    summary = {
        "total": len(items),
        "done": sum(1 for x in items if x.get("state") in ("SUCCESS", "FAILURE")),
    }
    return {"summary": summary, "items": items}


@router.get("/status/{job_id}")
async def get_task_status(job_id: str):
    """
    특정 job_id의 상태/진행률/결과 조회
    """
    ar = celery_app.AsyncResult(job_id)
    payload = _serialize_task(ar)
    return {"job_id": job_id, **payload}
