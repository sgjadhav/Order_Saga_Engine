from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base

DATABASE_URL = "postgresql+asyncpg://inventory_user:inventory_password@localhost:5434/inventory_service_db"

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True, pool_recycle=300)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session