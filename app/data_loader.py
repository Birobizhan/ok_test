import os
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
import logging
from datetime import date
from pandas import DataFrame
from .models import RespondentStats

logger = logging.getLogger(__name__)

csv_file = "data.csv"


async def load_csv_data(db: AsyncSession) -> None:
    """Чтение csv с помощью pandas и асинхронная вставка данных в БД"""
    if not os.path.exists(csv_file):
        logger.error(f"Файл {csv_file} не найден")
        return

    logger.info(f"Начало загрузки данных из {csv_file}")

    chunk_size: int = 10000
    total_rows: int = 0

    try:
        for i, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunk_size, sep=';')):
            chunk: DataFrame = chunk

            chunk.columns = chunk.columns.str.strip().str.lower()

            data_to_insert = []
            for _, row in chunk.iterrows():
                # Парсинг даты из yyyymmdd в объект date
                date_str: str = str(int(row['date']))
                date_obj = date(
                    int(date_str[:4]),
                    int(date_str[4:6]),
                    int(date_str[6:])
                )

                data_to_insert.append(RespondentStats(
                    date=date_obj,
                    respondent=int(row['respondent']),
                    sex=int(row['sex']),
                    age=int(row['age']),
                    weight=float(row['weight'])))

            db.add_all(data_to_insert)
            await db.commit()
            total_rows += len(data_to_insert)

            logger.info(f"Обработан чанк {i + 1}, всего строк: {total_rows}")

        logger.info(f"Данные успешно загружены, вставлено {total_rows} строк")

    except Exception as e:
        logger.exception(f"Произошла ошибка при загрузке данных: {e}")
        await db.rollback()
        raise
