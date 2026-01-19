import argparse
from config.config import const_url, const_props, error_logger, results_logger
import loader.db_parser as dbp
import spark_session as ss
from config.queries_list import QUERIES


def main(selected_queries):
    try:
        results_logger.info("MAIN FUNCTION STARTED")
        # Запуск Spark
        spark = ss.get_spark(app_name="Task4_Spark_App")
        results_logger.info("Spark session started")

        # Выполнение выбранных запросов
        for q_name in selected_queries:
            query_module = QUERIES.get(q_name)
            if query_module:
                results_logger.info(f"Running {q_name}...")
                query_module.get_queri_results() 
            else:
                error_logger.error(f"Query {q_name} not found")

        results_logger.info("MAIN FUNCTION FINISHED")
        print("__name__ =", __name__)

    except Exception as e:
        error_logger.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        spark.stop()
        results_logger.info("Spark application completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark queries CLI")
    parser.add_argument(
        "-q", "--queries",
        nargs="+",
        default=list(QUERIES.keys()),  # по умолчанию выполняем все
        help="List of queries to run, e.g., queri1 queri2"
    )
    args = parser.parse_args()

    main(args.queries)