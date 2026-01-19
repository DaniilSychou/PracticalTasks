from sys import exc_info
from unittest import result
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config.config import const_url, const_props, error_logger, results_logger
from loader.db_parser import load_table

def get_queri_results():

    try:
        # Load tables
        customer = load_table(const_url, "customer", const_props)
        rental = load_table(const_url, "rental", const_props)
        adress = load_table(const_url, "address", const_props)
        city = load_table(const_url, "city", const_props)
        inventory = load_table(const_url, "inventory", const_props)
        film = load_table(const_url, "film", const_props)
        film_actor = load_table(const_url, "film_actor", const_props)
        actor = load_table(const_url, "actor", const_props)

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
        .groupBy("city", F.col("title").alias("category"))  
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