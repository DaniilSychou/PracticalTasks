from unittest import result
import pyspark.sql.functions as F
from pyspark.sql.functions import count, col
from config.config import const_url, const_props, error_logger, results_logger
from loader.db_parser import load_table


def get_queri_results():

    try:
        # Load tables
        payment = load_table(const_url, "payment", const_props)
        rental = load_table(const_url, "rental", const_props)
        inventory = load_table(const_url, "inventory", const_props)
        film_category = load_table(const_url, "film_category", const_props)
        category = load_table(const_url, "category", const_props)

        result = (payment
        .join(rental, "rental_id")
        .join(inventory, "inventory_id")
        .join(film_category, "film_id")
        .join(category, "category_id")
        .groupBy("name")
        .agg(F.sum("amount").alias("total_revenue"))
        .withColumnRenamed("name", "category_name")
        .orderBy("total_revenue", ascending=False)
        .limit(1))

        results_logger.info("Query 3 executed successfully.")

        # Show results
        result.show()
        
    except Exception as e:
        error_logger.error(f"An error occurred in Query 3: {str(e)}", exc_info=True)
