# db_update_backend/routers/ohlcv_rest.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from db_module.connect_sqlalchemy_engine import get_async_db
from models import CryptoInfo

from celery.result import AsyncResult

# Celery 태스크/유틸 임포트
# - backfill_symbol_all_intervals_task : 실제 작업 큐잉
# - celery_app                         : 상태 조회용
# - normalize_symbol                   : 라우터에서도 한 번 더 정규화(이중 안전)
from tasks.backfill_tasks import (
    celery_app,
    backfill_symbol_all_intervals_task,
    normalize_symbol,
)

router = APIRouter(prefix="/ohlcv/rest", tags=["OHLCV-REST"])


# ------------------------------------------------------------------------------
# 내부 유틸: DB에서 등록된 모든 심볼 로드
# ------------------------------------------------------------------------------
async def _load_all_symbols(db: AsyncSession) -> List[str]:
    rows = (await db.execute(select(CryptoInfo.symbol))).scalars().all()
    # 중복 제거 및 깔끔 정렬(선택)
    symbols = sorted(set(map(str, rows)))
    return symbols


# ------------------------------------------------------------------------------
# 내부 유틸: Celery AsyncResult → 프론트가 쓰는 상태 페이로드로 직렬화
# ------------------------------------------------------------------------------
def _serialize_task(ar: AsyncResult) -> Dict[str, Any]:
    """
    프론트가 기대하는 필드셋으로 직렬화:
    - state: "PENDING" | "PROGRESS" | "SUCCESS" | "FAILURE" ...
    - status: 진행 텍스트(백엔드 meta['status'])
    - current: 현재까지 완료한 전체 인터벌 수
    - total: 전체 인터벌 수
    - interval_percentage: 현재 인터벌의 세부 진행률(0~100)
    - error_info: 실패 시 에러 메시지 (없으면 None)
    - symbol: 진행 중/대상 심볼(정규화된 값일 수 있음)
    """
    # 기본값
    state = ar.state or "PENDING"
    status = ""
    current = 0
    total = 0
    interval_percentage = 0.0
    error_info: Optional[str] = None
    symbol = None

    # Celery meta(ar.info)는 state에 따라 형태가 다름. dict일 때만 안전 추출.
    meta: Any
    try:
        meta = (
            ar.info
        )  # SUCCESS면 return 값, PROGRESS면 update_state(meta), FAILURE면 예외 객체/문자열 등
    except Exception as _:
        meta = None

    if isinstance(meta, dict):
        status = str(meta.get("status", "") or "")
        current = int(meta.get("current", 0) or 0)
        total = int(meta.get("total", 0) or 0)
        # interval_percentage는 float로 보장
        try:
            interval_percentage = float(meta.get("interval_percentage", 0.0) or 0.0)
        except Exception:
            interval_percentage = 0.0
        symbol = meta.get("symbol")
        # FAILURE에서도 meta가 dict일 수 있으니, error_info 우선 채우기
        if state == "FAILURE":
            # Celery의 표준 예외 포맷이 아닐 수 있으므로 최대한 친절히 직렬화
            error_info = meta.get("exc_message") or meta.get("error") or str(meta)
    else:
        # meta가 dict가 아니면 문자열화. (예외 객체 등)
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


# ------------------------------------------------------------------------------
# [POST] 모든 심볼 백필 시작
# - Body: { "intervals": ["1m","5m","15m","1h","4h","1d"] }  (선택)
# - 응답: { message, symbols_count, job_ids, symbol_job_map }
# ------------------------------------------------------------------------------
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

    # 라우터 레벨에서도 심볼 정규화(이중 안전)
    for raw in symbols:
        norm = normalize_symbol(raw)
        # Celery 큐잉 (kwargs에 모두 넣기: 시그니처 변화에 관대)
        async_result = backfill_symbol_all_intervals_task.apply_async(
            kwargs={"symbol": norm, "intervals": intervals}
        )
        job_ids.append(async_result.id)
        # 프론트는 원래 키(raw)로 상태를 묶기 때문에 raw→job_id로 반환
        symbol_job_map[raw] = async_result.id

    return {
        "message": "queued",
        "symbols_count": len(symbols),
        "job_ids": job_ids,
        "symbol_job_map": symbol_job_map,
    }


# ------------------------------------------------------------------------------
# [GET] 단일 작업 상태 조회
# ------------------------------------------------------------------------------
@router.get("/status/{job_id}")
async def get_task_status(job_id: str):
    ar = celery_app.AsyncResult(job_id)
    return _serialize_task(ar)


# ------------------------------------------------------------------------------
# [POST] 벌크 작업 상태 조회
# Body: { "job_ids": ["id1","id2", ...] }
# 응답: { summary: {total, done}, items: [{ job_id, ..._serialize_task } ...] }
# ------------------------------------------------------------------------------
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
