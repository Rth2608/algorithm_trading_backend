# models/crypto_info.py
from __future__ import annotations
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import SmallInteger, String
from .base import Base


class CryptoInfo(Base):
    __tablename__ = "crypto_info"
    __table_args__ = {"schema": "metadata"}  # ✅ 실제 스키마 반영

    symbol_id: Mapped[int] = mapped_column(
        SmallInteger, primary_key=True
    )  # 컬럼 옵션은 mapped_column에!
    symbol: Mapped[str] = mapped_column(String(30), unique=True, nullable=False)
