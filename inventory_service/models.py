from datetime import datetime
from sqlalchemy import String, Integer, Float, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from inventory_service.database import Base

class InventoryItem(Base):
    __tablename__ = "inventory_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    item_name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    available_stock: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )