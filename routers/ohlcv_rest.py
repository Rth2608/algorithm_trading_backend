from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from db_module.connect_sqlalchemy_engine import get_async_db
from models import CryptoInfo

from celery.result import AsyncResult

from tasks.backfill_tasks import (
    celery_app,
    backfill_symbol_all_intervals_task,
    normalize_symbol,
)

router = APIRouter(prefix="/ohlcv/rest", tags=["OHLCV-REST"])


async def _load_all_symbols(db: AsyncSession) -> List[str]:
    rows = (await db.execute(select(CryptoInfo.symbol))).scalars().all()
    symbols = sorted(set(map(str, rows)))
    return symbols


def _serialize_task(ar: AsyncResult) -> Dict[str, Any]:
    """
    프론트가 기대하는 필드셋으로 직렬화
    """
    state = ar.state or "PENDING"
    status = ""
    current = 0
    total = 0
    interval_percentage = 0.0
    error_info: Optional[str] = None
    symbol = None

    meta: Any
    try:
        meta = ar.info
    except Exception as _:
        meta = None

    if isinstance(meta, dict):
        status = str(meta.get("status", "") or "")
        current = int(meta.get("current", 0) or 0)
        total = int(meta.get("total", 0) or 0)
        try:
            interval_percentage = float(meta.get("interval_percentage", 0.0) or 0.0)
        except Exception:
            interval_percentage = 0.0
        symbol = meta.get("symbol")
        if state == "FAILURE":
            error_info = meta.get("exc_message") or meta.get("error") or str(meta)
    else:
        if state == "FAILURE":
            error_info = str(meta) if meta is not None else "Unknown error"

    return {
        "state": state,
        "status": status,
        "current": current,
        "total": total,
        "interval_percentage": interval_percentage,
        "error_info": error_info,
        "symbol": symbol,
    }


@router.post("/backfill/all-symbols")
async def trigger_backfill_all_symbols(
    intervals: Optional[List[str]] = Body(default=None, embed=True),
    db: AsyncSession = Depends(get_async_db),
):
    symbols = await _load_all_symbols(db)
    if not symbols:
        raise HTTPException(status_code=404, detail="등록된 심볼이 없습니다.")

    job_ids: List[str] = []
    symbol_job_map: Dict[str, str] = {}

    for raw in symbols:
        norm = normalize_symbol(raw)
        async_result = backfill_symbol_all_intervals_task.apply_async(
            kwargs={"symbol": norm, "intervals": intervals}
        )
        job_ids.append(async_result.id)
        symbol_job_map[raw] = async_result.id

    return {
        "message": "queued",
        "symbols_count": len(symbols),
        "job_ids": job_ids,
        "symbol_job_map": symbol_job_map,
    }


@router.get("/status/{job_id}")
async def get_task_status(job_id: str):
    ar = celery_app.AsyncResult(job_id)
    return _serialize_task(ar)


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
        "done": sum(1 for x in items if x["state"] in ("SUCCESS", "FAILURE")),
    }
    return {"summary": summary, "items": items}
