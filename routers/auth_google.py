# app/routers/auth_google.py

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
import httpx
import os
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from datetime import datetime, timezone

from db_module.connect_sqlalchemy_engine import get_async_db
from models import User
from core import auth

router = APIRouter(prefix="/auth/google", tags=["Authentication (Google)"])

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"
FRONTEND_URL = os.getenv("FRONTEND_URL")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@router.get("/callback")
async def google_callback(code: str, db: AsyncSession = Depends(get_async_db)):
    """구글 로그인 콜백을 처리하여 유저를 생성/로그인하고 JWT를 발급합니다."""

    logger.info("✅ [1] Google OAuth Callback 진입 — code 수신 완료")

    data = {
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": os.getenv("GOOGLE_REDIRECT_URI"),
    }

    try:
        # ✅ 2. Google Token API 요청
        logger.info("🌐 [2] Google Token 요청 시작")
        async with httpx.AsyncClient() as client:
            token_resp = await client.post(GOOGLE_TOKEN_URL, data=data)
            token_json = token_resp.json()
            logger.info(f"🔍 [2-1] Token 응답 상태코드: {token_resp.status_code}")
            logger.debug(f"🔍 [2-2] Token 응답 본문: {token_json}")

            if "access_token" not in token_json:
                logger.error(f"❌ [2-3] access_token 누락: {token_json}")
                raise HTTPException(
                    status_code=400, detail="Google access token 발급 실패"
                )

            access_token = token_json["access_token"]

            # ✅ 3. 사용자 정보 요청
            logger.info("👤 [3] Google UserInfo 요청 시작")
            user_resp = await client.get(
                GOOGLE_USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"}
            )
            user_info = user_resp.json()
            logger.info(f"🔍 [3-1] UserInfo 응답 상태코드: {user_resp.status_code}")
            logger.debug(f"🔍 [3-2] UserInfo 응답 본문: {user_info}")

    except httpx.RequestError as e:
        logger.exception("🚨 Google API 요청 실패")
        raise HTTPException(status_code=500, detail=f"Google API 요청 실패: {e}")

    # ✅ 4. 이메일 검증
    if not user_info.get("verified_email", False):
        logger.warning("⚠️ 이메일 인증이 안된 계정입니다.")
        raise HTTPException(
            status_code=400, detail="Google 이메일 인증이 되지 않은 계정입니다."
        )

    email = user_info["email"]
    google_id = user_info["id"]
    google_name = user_info.get("name")

    logger.info(f"📩 [4] 사용자 정보 수신 완료 — {email}")

    # ✅ 5. DB 조회
    result = await db.execute(select(User).filter_by(email=email))
    user = result.scalar_one_or_none()
    is_new_user = False

    if user:
        logger.info(f"👤 [5] 기존 유저 로그인: {user.email}")
        if not user.google_id:
            user.google_id = google_id
        if not user.username:
            user.username = google_name
        user.last_login = datetime.now(timezone.utc)
    else:
        logger.info(f"🆕 [5] 신규 유저 생성: {email}")
        is_new_user = True
        new_user = User(
            email=email,
            username=google_name,
            google_id=google_id,
            is_active=True,
            is_admin=False,
            email_opt_in=False,
        )
        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)
        user = new_user

    # ✅ 6. JWT 생성
    try:
        token = auth.create_access_token(data={"sub": user.email, "id": user.user_id})
        logger.info("🔑 [6] JWT 발급 성공")
    except Exception as e:
        logger.exception("🚨 JWT 발급 실패")
        raise HTTPException(status_code=500, detail=f"JWT 생성 실패: {e}")

    # ✅ 7. 프론트엔드 리다이렉트
    if is_new_user:
        redirect_url = f"{FRONTEND_URL}/interest?token={token}"
    else:
        redirect_url = f"{FRONTEND_URL}/main?token={token}"

    logger.info(f"🚀 [7] Redirect: {redirect_url}")
    return RedirectResponse(url=redirect_url)
