from pyspark.sql import functions as F
from config.config import const_url, const_props, error_logger, results_logger
from loader.db_parser import load_table

def get_queri_results():

    try:
        film_category = load_table(const_url, "film_category", const_props)
        category = load_table(const_url, "category", const_props)
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
