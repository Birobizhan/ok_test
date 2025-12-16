import asyncio
import logging
from sqlalchemy import select
from .models import RespondentStats
from .database import engine, AsyncSessionLocal, Base
from .data_loader import load_csv_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)

logger = logging.getLogger(__name__)


async def init_db_data() -> None:
    """Функция инициализации базы данных и заполнения ее данными"""
    logger.info("Запуск инициализации БД")

    try:
        # Создание таблиц в БД если их нет
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Таблицы созданы/проверены")

        # Проверка наличия данных и загрузка если нужно
        async with AsyncSessionLocal() as db:
            
            result = await db.execute(select(RespondentStats).limit(1))
            first_row = result.scalar_one_or_none()
            
            if first_row is None:
                logger.info("Таблица с данными пуста, начало заполнения данными")
                await load_csv_data(db)
            else:
                logger.info("В таблице уже есть данные")

        logger.info("Инициализация БД завершена")

    except Exception as e:
        logger.error(f"Ошибка при инициализации БД: {e}", exc_info=True)
        raise


async def main():
    """Точка входа для скрипта инициализации"""
    try:
        await init_db_data()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
