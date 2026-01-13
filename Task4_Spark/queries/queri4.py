from pyspark.sql.functions import count, col
from config.config import error_logger, results_logger


def get_queri_results(ds: dict):

    try:    
        # Load tables
        inventory = ds['inventory']
        film = ds['film']

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