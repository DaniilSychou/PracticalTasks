from pyspark.sql.functions import count, col
from config.config import const_url, const_props, error_logger, results_logger
from loader.db_parser import load_table


def get_queri_results():

    try:    
        # Load tables
        inventory = load_table(const_url, "inventory", const_props)
        film = load_table(const_url, "film", const_props)

        # Join and aggregate
        result = film.join(
            inventory,
            film.film_id == inventory.film_id,
            "left"
        ).filter(inventory.film_id.isNull()).select(film.title)

        results_logger.info("Query 4 executed successfully.")

        #Show result
        result.show()
    except Exception as e:
        error_logger.error(f"An error occurred in Query 4: {str(e)}", exc_info=True)