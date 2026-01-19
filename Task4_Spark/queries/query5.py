from pyspark.sql.functions import count, col
from config.config import const_url, const_props, error_logger, results_logger
from loader.db_parser import load_table

def get_queri_results():

    try:
        # Load tables
        category = load_table(const_url, "category", const_props)
        film_category = load_table(const_url, "film_category", const_props)
        film = load_table(const_url, "film", const_props)
        film_actor = load_table(const_url, "film_actor", const_props)
        actor = load_table(const_url, "actor", const_props)

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

