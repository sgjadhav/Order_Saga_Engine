import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

# PostgreSQL connection string for order-db container
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://order_user:order_password@127.0.0.1:5433/order_service_db"
)

engine = create_async_engine(DATABASE_URL, echo=True)

# Async session factory to handle DB transactions
AsyncSessionLocal = async_sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session