from pyspark.sql.functions import count, col
from config.config import const_url, const_props, error_logger, results_logger
from loader.db_parser import load_table

def get_queri_results():

    try:
        # Load tables
        rental = load_table(const_url, "rental", const_props)
        inventory = load_table(const_url, "inventory", const_props)
        film = load_table(const_url, "film", const_props)
        film_actor = load_table(const_url, "film_actor", const_props)
        actor = load_table(const_url, "actor", const_props)

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
