import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# Получение DATABASE_URL из переменных окружения, если их нет, то подставляется заранее приготовленный url
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/stats_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


# Функция для получения сессии в эндпоинтах
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
