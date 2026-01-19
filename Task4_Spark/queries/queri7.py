from sys import exc_info
from unittest import result
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config.config import results_logger, error_logger


def get_queri_results(ds: dict):

    try:
        # Load tables
        customer = ds['customer']
        rental = ds['rental']
        adress = ds['address']
        city = ds['city']
        inventory = ds['inventory']
        film = ds['film']
        film_actor = ds['film_actor']
        actor = ds['actor']

        # Join and aggregate
        rental_hours = (rental
        .join(inventory, "inventory_id")
        .join(film, "film_id")
        .join(film_actor, "film_id")
        .join(actor, "actor_id")
        .join(customer, "customer_id")
        .join(adress, "address_id")
        .join(city, "city_id")
        .withColumn("total_hours", 
            (F.col("return_date").cast("long") - F.col("rental_date").cast("long")) / 3600)
        .filter((F.col("city").rlike("(?i).*a.*")) | (F.col("city").rlike(".*-.*")))
        .groupBy("city", F.col("title").alias("category"))  # <- вместо name используем title
        .agg(F.sum("total_hours").alias("total_hours"))
    )
        # ranked_categories
        window_spec = Window.partitionBy("city").orderBy(F.desc("total_hours"))
        ranked = (rental_hours
            .withColumn("rnk", F.dense_rank().over(window_spec)))

        # final result
        result = (ranked
            .filter(F.col("rnk") == 1)
            .withColumn("city_type", 
                F.when(F.col("city").rlike(".*-.*"), "city-with-dash")
                .otherwise("city-with-a"))
            .select("city", "category", F.round("total_hours", 2).alias("total_hours"), "city_type")
            .orderBy("city"))

        results_logger.info("Query 7 executed successfully.")

        #Show result
        result.show()

    except Exception as e:
        error_logger.error(f"Error in Query 7: {str(e)}", exc_info=True)