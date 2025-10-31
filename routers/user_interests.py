from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from pydantic import BaseModel
from typing import List

from db_module.connect_sqlalchemy_engine import get_async_db
from models import User

from core import auth  # 4번 auth.py 파일

router = APIRouter(prefix="/user", tags=["User"])


class InterestRequest(BaseModel):
    selectedIndices: List[str]
    selectedStocks: List[str]
    email_opt_in: bool


@router.post("/register-complete")
async def register_complete(
    payload: InterestRequest,
    db: AsyncSession = Depends(get_async_db),
    current_user: User = Depends(auth.get_current_user),
):
    """현재 로그인된 유저의 관심 종목 및 이메일 수신 동의를 업데이트합니다."""

    current_user.email_opt_in = payload.email_opt_in

    await db.commit()
    return {"msg": "정보 업데이트가 완료되었습니다"}
