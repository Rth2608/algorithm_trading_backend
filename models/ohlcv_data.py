# models/ohlcv_data.py
from __future__ import annotations
from datetime import datetime
from sqlalchemy import SmallInteger, TIMESTAMP, Numeric, Boolean, Enum, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

# DB의 타입: trading_data.ohlcv_source ENUM('REST','WS') 에 직접 매핑
ohlcv_source_enum = Enum(
    "REST",
    "WS",
    name="ohlcv_source",
    schema="trading_data",
)


class _OhlcvBase(Base):
    """공통 컬럼(복합 PK: symbol_id, timestamp)"""

    __abstract__ = True
    __table_args__ = {"schema": "trading_data"}  # ✅ 모든 파생 클래스에 스키마 적용

    symbol_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("metadata.crypto_info.symbol_id", ondelete="CASCADE"),
        primary_key=True,
    )
    timestamp: Mapped[datetime] = mapped_column(
        "timestamp", TIMESTAMP(timezone=True), primary_key=True
    )

    open: Mapped[float] = mapped_column(Numeric(20, 7), nullable=False)
    high: Mapped[float] = mapped_column(Numeric(20, 7), nullable=False)
    low: Mapped[float] = mapped_column(Numeric(20, 7), nullable=False)
    close: Mapped[float] = mapped_column(Numeric(20, 7), nullable=False)

    volume: Mapped[float] = mapped_column(Numeric(20, 3), nullable=False)

    # DB enum 사용
    src: Mapped[str] = mapped_column(ohlcv_source_enum, nullable=False, default="REST")

    is_ended: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# 각 인터벌별 테이블
class Ohlcv1m(_OhlcvBase):
    __tablename__ = "ohlcv_1m"


class Ohlcv5m(_OhlcvBase):
    __tablename__ = "ohlcv_5m"


class Ohlcv15m(_OhlcvBase):
    __tablename__ = "ohlcv_15m"


class Ohlcv1h(_OhlcvBase):
    __tablename__ = "ohlcv_1h"


class Ohlcv4h(_OhlcvBase):
    __tablename__ = "ohlcv_4h"


class Ohlcv1d(_OhlcvBase):
    __tablename__ = "ohlcv_1d"
