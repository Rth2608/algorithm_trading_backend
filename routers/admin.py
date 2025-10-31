from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from loguru import logger
from pathlib import Path
import pandas as pd

from db_module.connect_sqlalchemy_engine import get_async_db
from models import CryptoInfo

router = APIRouter(prefix="/admin", tags=["Admin"])

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "initial_settings" / "symbol_data" / "symbols.csv"


@router.post("/register_symbols")
async def register_symbols(db: AsyncSession = Depends(get_async_db)):
    """
    initial_settings/symbol_data/symbols.csv에서 'symbol' column을 읽어
    metadata.crypto_info(symbol PK)로 upsert 등록
    """
    try:
        logger.info(f"CSV 경로 확인: {CSV_PATH}")

        if not CSV_PATH.exists():
            msg = f"CSV 파일이 없습니다: {CSV_PATH}"
            logger.error(msg)
            raise HTTPException(status_code=404, detail=msg)

        df = pd.read_csv(CSV_PATH)
        logger.info(f"CSV 로드 완료: {len(df)}개 행")

        if "symbol" not in df.columns:
            msg = "CSV에 'symbol' 컬럼이 없습니다."
            logger.error(msg)
            raise HTTPException(status_code=400, detail=msg)

        s = (
            df["symbol"]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({"": None})
            .dropna()
        )
        s = s[s.str.len() <= 30].drop_duplicates()

        if s.empty:
            msg = "등록할 심볼이 없습니다(전처리 후 빈 목록)."
            logger.warning(msg)
            return {"message": msg, "attempted": 0}

        data = [{"symbol": v} for v in s.tolist()]
        logger.info(f"등록 대상: {len(data)}개 symbol")

        stmt = (
            insert(CryptoInfo)
            .values(data)
            .on_conflict_do_nothing(index_elements=["symbol"])
        )

        await db.execute(stmt)
        await db.commit()

        msg = f"{len(data)}개 심볼 등록 시도 (기존 중복은 무시됨)"
        logger.success(msg)
        return {"message": msg, "attempted": len(data)}

    except HTTPException:
        raise
    except Exception as e:
        msg = f"심볼 등록 중 오류 발생: {e}"
        logger.exception(msg)
        raise HTTPException(status_code=500, detail=msg)
