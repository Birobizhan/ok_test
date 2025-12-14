import re

# Список разрешенных имен столбцов
allowed_columns = {'date', 'respondent', 'sex', 'age'}

# Черный список с опасными ключевыми словами и символами
dangerous_words = r'\b(DROP|DELETE|UPDATE|INSERT|ALTER|UNION|TRUNCATE|CREATE|EXEC|CALL)\b'
dangerous_symbol = r'[;]'


def validate_sql_condition(condition: str) -> str:
    """Проверяет, что строка содержит только безопасное SQL-условие WHERE"""
    if re.search(dangerous_symbol, condition):
        raise ValueError("Условие содержит запрещенные символы")

    if re.search(dangerous_words, condition, re.IGNORECASE):
        raise ValueError("Условие содержит запрещенные SQL-команды")

    # Проверка на использование только разрешенных столбцов

    # Поиск всех слов и проверка того, что они в белом списке
    words = re.findall(r'[a-zA-Z_]+', condition)

    # Разрешенные операторы и функции, которые не являются именами столбцов
    allowed_operators = {'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'BETWEEN', 'LIKE', 'DATE', 'INT', 'FLOAT'}

    for word in words:
        upper_word = word.upper()
        if upper_word not in allowed_operators and word.lower() not in allowed_columns:
            raise ValueError(f"Использование неразрешенного имени столбца или ключевого слова: {word}")

    return condition
