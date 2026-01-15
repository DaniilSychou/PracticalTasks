from pyspark.sql.functions import count, col
from config.config import error_logger, results_logger


def get_queri_results(ds: dict):

    try:
        # Load tables
        category = ds['category']
        film_category = ds['film_category']
        film = ds['film']
        film_actor = ds['film_actor']
        actor = ds['actor']

        # Join and aggregate
        result = (actor
            .join(film_actor, actor.actor_id == film_actor.actor_id)
            .join(film, film.film_id == film_actor.film_id)
            .join(film_category, film_category.film_id == film.film_id)
            .join(category, category.category_id == film_category.category_id)
            .filter(col("name") == "Children")
            .groupBy(actor.actor_id, actor.first_name, actor.last_name)
            .agg(count(film.film_id).alias("films_count"))
            .orderBy(col("films_count").desc())
            .limit(3)
            .select("first_name", "last_name", "films_count"))

        results_logger.info("Query 5 executed successfully.")

        # Show results
        result.show()
    except Exception as e:
        error_logger.error(f"Error in Query 5: {str(e)}", exc_info=True)

