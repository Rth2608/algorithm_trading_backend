from __future__ import annotations
from datetime import datetime

from sqlalchemy import (
    String,
    TIMESTAMP,
    Numeric,
    Boolean,
    Enum,
    ForeignKey,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

ohlcv_source_enum = Enum(
    "REST",
    "WS",
    name="ohlcv_source",
    schema="trading_data",
)


class _OhlcvBase(Base):
    """공통 컬럼(복합 PK: symbol, timestamp)"""

    __abstract__ = True
    __table_args__ = {"schema": "trading_data"}

    symbol: Mapped[str] = mapped_column(
        String(30),
        ForeignKey("metadata.crypto_info.symbol", ondelete="CASCADE"),
        primary_key=True,
    )
    timestamp: Mapped[datetime] = mapped_column(
        "timestamp",
        TIMESTAMP(timezone=True),
        primary_key=True,
    )

    open: Mapped[float] = mapped_column(Numeric(20, 7), nullable=False)
    high: Mapped[float] = mapped_column(Numeric(20, 7), nullable=False)
    low: Mapped[float] = mapped_column(Numeric(20, 7), nullable=False)
    close: Mapped[float] = mapped_column(Numeric(20, 7), nullable=False)

    volume: Mapped[float] = mapped_column(Numeric(20, 3), nullable=False)

    src: Mapped[str] = mapped_column(ohlcv_source_enum, nullable=False, default="REST")
    is_ended: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


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
