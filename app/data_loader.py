import os
import pandas as pd
from sqlalchemy.orm import Session
import logging
from pandas import DataFrame

from .database import engine, SessionLocal, Base
from .models import RespondentStats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

CSV_FILE = "data.csv"


def load_csv_data(db: Session) -> None:
    """Чтение csv с помощью pandas и вставка данных в бд"""
    if not os.path.exists(CSV_FILE):
        logger.error(f"Файл {CSV_FILE} не найден. Загрузка отменена.")
        return

    logger.info(f"Начало загрузки данных из {CSV_FILE}")

    chunk_size: int = 10000
    total_rows: int = 0

    try:
        for i, chunk in enumerate(pd.read_csv(CSV_FILE, chunksize=chunk_size, sep=';')):
            chunk: DataFrame = chunk

            chunk.columns = chunk.columns.str.strip().str.lower()

            data_to_insert = []
            for _, row in chunk.iterrows():
                # Парсинг даты из yyyymmdd в yyyy-mm-dd
                date_str: str = str(int(row['date']))
                formatted_date: str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

                data_to_insert.append(RespondentStats(
                    date=formatted_date,
                    respondent=int(row['respondent']),
                    sex=int(row['sex']),
                    age=int(row['age']),
                    weight=float(row['weight'])))

            db.add_all(data_to_insert)
            db.commit()
            total_rows += len(data_to_insert)

            logger.debug(f"Обработан чанк {i + 1}. Всего строк: {total_rows}")

        logger.info(f"Данные успешно загружены. Вставлено {total_rows} строк")

    except Exception as e:
        logger.exception(f"Произошла ошибка при загрузке данных: {e}")
        db.rollback()


def init_db_data() -> None:
    """Функция инициализации базы данных и заполнения ее данными """
    logger.info("Запуск инициализации бд")

    # Создание таблиц в бд если их нет
    Base.metadata.create_all(bind=engine)

    with SessionLocal() as db:
        db: Session = db
        if db.query(RespondentStats).first() is None:
            logger.info("Таблица с данными пуста, начало заполнения ее данными")
            load_csv_data(db)
        else:
            logger.info("В таблице уже есть данные")

    logger.info("Инициализация бд завершена")
