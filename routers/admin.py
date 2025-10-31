from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from loguru import logger
from pathlib import Path
import pandas as pd

from db_module.connect_sqlalchemy_engine import get_async_db
from models import CryptoInfo  # metadata.crypto_info 모델

router = APIRouter(prefix="/admin", tags=["Admin"])

BASE_DIR = Path(__file__).resolve().parent.parent  # /app/backend
CSV_PATH = BASE_DIR / "initial_settings" / "symbol_data" / "symbols.csv"


@router.post("/register_symbols")
async def register_symbols(db: AsyncSession = Depends(get_async_db)):
    """
    서버 내부 CSV (initial_settings/symbol_data/symbols.csv)에서 symbol 컬럼만 DB에 등록
    """
    try:
        logger.info(f"📂 CSV 경로 확인: {CSV_PATH}")

        if not CSV_PATH.exists():
            msg = f"❌ CSV 파일이 없습니다: {CSV_PATH}"
            logger.error(msg)
            return {"message": msg}

        df = pd.read_csv(CSV_PATH)
        logger.info(f"✅ CSV 로드 완료: {len(df)}개 행")

        if "symbol" not in df.columns:
            msg = "❌ CSV에 'symbol' 컬럼이 없습니다."
            logger.error(msg)
            return {"message": msg}

        # 중복 제거 및 symbol 리스트 추출
        df = df[["symbol"]].drop_duplicates()
        data = df.to_dict(orient="records")

        logger.info(f"📊 등록 대상: {len(data)}개 symbol")

        # PostgreSQL 전용 INSERT
        stmt = insert(CryptoInfo).values(data)
        stmt = stmt.on_conflict_do_nothing(index_elements=["symbol"])

        await db.execute(stmt)
        await db.commit()

        msg = f"✅ {len(data)}개 심볼 등록 완료"
        logger.success(msg)
        return {"message": msg}

    except Exception as e:
        msg = f"❌ 심볼 등록 중 오류 발생: {e}"
        logger.exception(msg)
        return {"message": msg}
