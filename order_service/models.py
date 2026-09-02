from datetime import datetime
from typing import Optional
from sqlalchemy import String, Integer, Float, DateTime, func, Text
from sqlalchemy.orm import Mapped, mapped_column
from pydantic import BaseModel, ConfigDict, Field

try:
    from .database import Base
except ImportError:
    from database import Base

# 1. Main Order Model
class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    item_name: Mapped[str] = mapped_column(String(100), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="PENDING")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

# 2. Transactional Outbox Model (Crucial for Relay)
class OutboxMessage(Base):
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    processed: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

# Pydantic Schemas
class OrderCreate(BaseModel):
    item_name: str = Field(..., examples=["Laptop"])
    quantity: int = Field(default=1, examples=[2])
    price: float = Field(..., gt=0, examples=[999.99])

class OrderUpdate(BaseModel):
    item_name: Optional[str] = Field(default=None, examples=["Laptop Pro"])
    quantity: Optional[int] = Field(default=None, ge=1, examples=[3])
    price: Optional[float] = Field(default=None, gt=0, examples=[1199.99])
    status: Optional[str] = Field(default=None, examples=["COMPLETED"])

class OrderResponse(BaseModel):
    id: int
    item_name: str
    quantity: int
    price: float
    status: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)