from pyspark.sql.functions import count, col
from config.config import error_logger, results_logger


def get_queri_results(ds: dict):

    try:
        # Load tables
        rental = ds['rental']
        inventory = ds['inventory']
        film = ds['film']
        film_actor = ds['film_actor']
        actor = ds['actor']
        
        # Join and aggregate
        result = (rental
            .join(inventory, "inventory_id")
            .join(film, "film_id")
            .join(film_actor, "film_id")
            .join(actor, "actor_id")
            .groupBy("actor_id", "first_name", "last_name")
            .agg(count("rental_id").alias("rental_count"))
            .orderBy(col("rental_count").desc())
            .limit(10))

        results_logger.info("Query 2 executed successfully.")

        # Show results
        result.show()
    except Exception as e:
        error_logger.error(f"Error executing Query 2: {str(e)}", exc_info=True)
