# Альтернативный sql-запрос, если учитывать, что у респондента может измениться возраст в разные даты
# В исходных данных я таких примеров не нашел, но если подразумевается такая возможность, то нужно использовать этот запрос
# Он отличается тем, что сначала считает среднее для каждого респондента и только потом фильтрует их в зависимости от запроса


# sql_query = text(f"""
#     WITH avg_weights AS (
#         SELECT
#             respondent,
#             AVG(weight) as overall_avg_w
#         FROM respondent_stats
#         GROUP BY respondent
#     ),
#     aud1 AS (
#         SELECT
#             T1.respondent,
#             T2.overall_avg_w as avg_w
#         FROM respondent_stats T1
#         JOIN avg_weights T2 ON T1.respondent = T2.respondent
#         WHERE {safe_audience1}
#         GROUP BY T1.respondent, T2.overall_avg_w
#     ),
#     aud2 AS (
#         SELECT respondent
#         FROM respondent_stats
#         WHERE {safe_audience2}
#         GROUP BY respondent
#     )
#     SELECT
#         COALESCE(SUM(T1.avg_w), 0) as total_weight,
#         COALESCE(SUM(CASE WHEN T2.respondent IS NOT NULL THEN T1.avg_w ELSE 0 END), 0) as intersection_weight
#     FROM aud1 T1
#     LEFT JOIN aud2 T2 ON T1.respondent = T2.respondent;
#     """)
