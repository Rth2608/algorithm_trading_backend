from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String
from .base import Base


class CryptoInfo(Base):
    __tablename__ = "crypto_info"
    __table_args__ = {"schema": "metadata"}

    symbol: Mapped[str] = mapped_column(String(30), primary_key=True)

    def __repr__(self) -> str:
        return f"CryptoInfo(symbol={self.symbol!r})"
