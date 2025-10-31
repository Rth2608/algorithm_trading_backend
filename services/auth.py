from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from pydantic import BaseModel, ValidationError
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from db_module.connect_sqlalchemy_engine import get_async_db
from models import User

security = HTTPBearer()

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))


class TokenData(BaseModel):
    user_id: Optional[int] = None


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """우리 시스템의 JWT 액세스 토큰을 생성합니다."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=ACCESS_TOKEN_EXPIRE_MINUTES
        )
    to_encode.update({"exp": expire})

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> TokenData:
    """JWT 토큰을 검증하고 user_id가 포함된 TokenData를 반환합니다."""
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

        user_id: int = payload.get("id")
        email: str = payload.get("sub")

        if user_id is None or email is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="토큰 페이로드가 유효하지 않습니다.",
            )
        token_data = TokenData(user_id=user_id)

    except (JWTError, ValidationError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="토큰이 유효하지 않습니다."
        )
    return token_data


async def get_current_user(
    db: AsyncSession = Depends(get_async_db),
    token_data: TokenData = Depends(verify_token),
) -> User:
    """토큰에서 user_id를 가져와 DB에서 현재 유저를 비동기로 조회합니다."""

    result = await db.execute(select(User).filter_by(user_id=token_data.user_id))
    user = result.scalar_one_or_none()

    if user is None or user.is_active == False:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="사용자를 찾을 수 없거나 활성화되지 않았습니다.",
        )
    return user
