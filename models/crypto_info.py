from __future__ import annotations
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import SmallInteger, String
from .base import Base


class CryptoInfo(Base):
    __tablename__ = "crypto_info"
    __table_args__ = {"schema": "metadata"}

    symbol_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(30), unique=True, nullable=False)
