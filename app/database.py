import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

# Получение DATABASE_URL из переменных окружения, если их нет, то подставляется заранее приготовленный url
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@localhost:5432/stats_db")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, autocommit=False, autoflush=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    """Функция для получения async сессии в эндпоинтах"""
    async with AsyncSessionLocal() as session:
        yield session
