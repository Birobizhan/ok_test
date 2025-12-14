import logging
from fastapi import HTTPException, Query, Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.orm import Session

from .database import get_db
from .data_loader import init_db_data
from .validate_sql import validate_sql_condition

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = FastAPI()


@app.on_event("startup")
def on_startup() -> None:
    """Инициализация и заполнение данными бд при запуске"""
    logger.info("Запуск обработчика startup")
    init_db_data()
    logger.info("Инициализация завершена")


@app.get("/getPercent")
def get_percent(
        audience1: str = Query(..., description="SQL условие для первой аудитории"),
        audience2: str = Query(..., description="SQL условие для второй аудитории"),
        db: Session = Depends(get_db)
) -> dict[str, float]:
    """Эндпоинт для получения процента вхождения второй аудитории в первую, валидирует оба запроса к аудиториям и потом
    посылает запрос в БД для получения людей входящих в аудитории и затем расчет процента вхождения"""
    logger.debug(f"Получен запрос: aud1='{audience1}', aud2='{audience2}'")

    try:
        # Применение валидации
        safe_audience1: str = validate_sql_condition(audience1)
        safe_audience2: str = validate_sql_condition(audience2)
        logger.debug("Валидация SQL-условий прошла успешно.")

    except ValueError as e:
        logger.warning(f"Ошибка валидации ввода: {e}")
        raise HTTPException(status_code=400, detail=f"Недопустимый ввод: {str(e)}")

    sql_query = text(f"""
    WITH aud1 AS (
        SELECT respondent, AVG(weight) as avg_w
        FROM respondent_stats
        WHERE {safe_audience1}
        GROUP BY respondent
    ),
    aud2 AS (
        SELECT respondent
        FROM respondent_stats
        WHERE {safe_audience2}
        GROUP BY respondent
    )
    SELECT 
        COALESCE(SUM(avg_w), 0) as total_weight,
        COALESCE(SUM(CASE WHEN respondent IN (SELECT respondent FROM aud2) THEN avg_w ELSE 0 END), 0) as intersection_weight
    FROM aud1;
    """)

    try:
        result = db.execute(sql_query).fetchone()

        if result is None:
            total_weight: float = 0.0
            intersection_weight: float = 0.0
        else:
            total_weight, intersection_weight = result

        total_weight = float(total_weight)
        intersection_weight = float(intersection_weight)

        if total_weight == 0:
            logger.info("Общий вес аудитории 0")
            return {"percent": 0.0}

        percent: float = intersection_weight / total_weight
        logger.info(f"Расчет завершен: {intersection_weight}/{total_weight} = {percent}")

        return {"percent": percent}

    except Exception as e:
        logger.error(f"Ошибка выполнения SQL-запроса: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Query error: {str(e)}")
