from pyspark.sql import functions as F
from config.config import error_logger, results_logger

def get_queri_results(ds: dict):

    try:
        film_category = ds['film_category']
        category = ds['category']

        # Join and aggregate
        result = (
        film_category
        .join(category, "category_id")        # соединяем с категориями
        .groupBy("name")                            # группируем по имени категории
        .agg(F.count("film_id").alias("count_films"))  # считаем количество фильмов
        .sort(F.desc("count_films"))                # сортируем по убыванию
        )
        
        results_logger.info("Query 1 executed successfully.")
        
        # Display results
        result.show()
    except Exception as e:
        error_logger.error(f"Error executing Query 1: {str(e)}", exc_info=True)
