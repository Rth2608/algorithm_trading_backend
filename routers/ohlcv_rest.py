from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from celery.result import AsyncResult  # ← 필요

from db_module.connect_sqlalchemy_engine import get_async_db
from models import CryptoInfo

from tasks.backfill_tasks import (
    celery_app,
    backfill_symbol_all_intervals_task,
    normalize_symbol,
)

router = APIRouter(prefix="/ohlcv/rest", tags=["OHLCV-REST"])


async def _load_all_symbols(db: AsyncSession) -> List[str]:
    rows = (await db.execute(select(CryptoInfo.symbol))).scalars().all()
    return sorted(set(map(str, rows)))


def _serialize_task(ar: AsyncResult) -> Dict[str, Any]:
    try:
        state = ar.state or "PENDING"
    except Exception:
        state = "PENDING"

    status = ""
    current = 0
    total = 0
    interval_percentage = 0.0
    error_info: Optional[str] = None
    symbol: Optional[str] = None
    interval: Optional[str] = None

    chunk_current: Optional[int] = None
    chunk_total: Optional[int] = None
    chunk_pct: Optional[float] = None

    try:
        meta = ar.info
    except Exception:
        meta = None

    if isinstance(meta, dict):
        status = str(meta.get("status") or "")
        try:
            current = int(meta.get("current") or 0)
        except Exception:
            current = 0
        try:
            total = int(meta.get("total") or 0)
        except Exception:
            total = 0
        try:
            interval_percentage = float(meta.get("interval_percentage") or 0.0)
        except Exception:
            interval_percentage = 0.0

        symbol = meta.get("symbol")
        interval = meta.get("interval")

        try:
            chunk_current = (
                int(meta.get("chunk_current"))
                if meta.get("chunk_current") is not None
                else None
            )
        except Exception:
            chunk_current = None
        try:
            chunk_total = (
                int(meta.get("chunk_total"))
                if meta.get("chunk_total") is not None
                else None
            )
        except Exception:
            chunk_total = None
        try:
            chunk_pct = (
                float(meta.get("chunk_pct"))
                if meta.get("chunk_pct") is not None
                else None
            )
        except Exception:
            chunk_pct = None

        if state == "FAILURE":
            error_info = meta.get("exc_message") or meta.get("error") or str(meta)
    else:
        if state == "FAILURE":
            error_info = str(meta) if meta is not None else "Unknown error"

    if (
        (chunk_pct is None or chunk_pct == 0.0)
        and chunk_current is not None
        and chunk_total
        and chunk_total > 0
    ):
        try:
            chunk_pct = min(100.0, (float(chunk_current) / float(chunk_total)) * 100.0)
        except Exception:
            chunk_pct = 0.0

    return {
        "state": state,
        "status": status,
        "current": current,
        "total": total,
        "interval_percentage": interval_percentage,
        "error_info": error_info,
        "symbol": symbol,
        "interval": interval,
        "chunk_current": chunk_current or 0,
        "chunk_total": chunk_total or 0,
        "chunk_pct": chunk_pct or 0.0,
    }


def _default_intervals(intervals: Optional[List[str]]) -> List[str]:
    return intervals or ["1m", "5m", "15m", "1h", "4h", "1d"]


@router.post("/backfill/all-symbols")
async def trigger_backfill_all_symbols(
    intervals: Optional[List[str]] = Body(default=None, embed=True),
    db: AsyncSession = Depends(get_async_db),
):
    symbols = await _load_all_symbols(db)
    if not symbols:
        raise HTTPException(status_code=404, detail="등록된 심볼이 없습니다.")

    use_intervals = _default_intervals(intervals)

    job_ids: List[str] = []
    symbol_job_map: Dict[str, str] = {}

    for raw in symbols:
        norm = normalize_symbol(raw)
        ar = backfill_symbol_all_intervals_task.apply_async(
            kwargs={"symbol": norm, "intervals": use_intervals}
        )
        job_ids.append(ar.id)
        symbol_job_map[raw] = ar.id

    return {
        "message": "queued",
        "symbols_count": len(symbols),
        "intervals": use_intervals,
        "job_ids": job_ids,
        "symbol_job_map": symbol_job_map,
    }


@router.post("/backfill/symbol")
async def trigger_backfill_symbol(
    symbol: str = Body(..., embed=True),
    intervals: Optional[List[str]] = Body(default=None, embed=True),
):
    use_intervals = _default_intervals(intervals)
    norm = normalize_symbol(symbol)

    ar = backfill_symbol_all_intervals_task.apply_async(
        kwargs={"symbol": norm, "intervals": use_intervals}
    )
    return {
        "message": "queued",
        "symbol_input": symbol,
        "symbol_used": norm,
        "intervals": use_intervals,
        "job_id": ar.id,
    }


@router.get("/status/{job_id}")
async def get_task_status(job_id: str):
    ar = celery_app.AsyncResult(job_id)
    payload = _serialize_task(ar)
    return {"job_id": job_id, **payload}


@router.post("/status/bulk")
async def get_task_status_bulk(job_ids: List[str] = Body(..., embed=True)):
    if not job_ids:
        raise HTTPException(status_code=422, detail="job_ids가 비어 있습니다.")

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
