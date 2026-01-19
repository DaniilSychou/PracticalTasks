from unittest import result
from pyspark.sql.functions import count, col
from config.config import error_logger, results_logger


def get_queri_results(ds: dict):

    try:
        # Load tables
        payment = ds['payment']
        rental = ds['rental']
        inventory = ds['inventory']
        film_category = ds['film_category']
        category = ds['category']

        result = (payment
        .join(rental, "rental_id")
        .join(inventory, "inventory_id")
        .join(film_category, "film_id")
        .join(category, "category_id")
        .groupBy("name")
        .agg({"amount": "sum"})
        .withColumnRenamed("name", "category_name")
        .withColumnRenamed("sum(amount)", "total_revenue")
        .sort("total_revenue", ascending=False)
        .limit(1))

        results_logger.info("Query 3 executed successfully.")

        # Show results
        result.show()
        
    except Exception as e:
        error_logger.error(f"An error occurred in Query 3: {str(e)}", exc_info=True)
