import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import pymongo

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

DB_NAME = "textdb"
COLLECTION_NAME = "processed_data"
CSV_PATH = "/opt/airflow/data/processed_tiktok_reviews.csv"

BATCH_SIZE = 10000  # размер чанка

processed_reviews_dataset = Dataset(CSV_PATH)

def load_to_mongo(**context):
    client = None
    try:
        mongo_hook = MongoHook(mongo_conn_id="mongo_connect")
        client = mongo_hook.get_conn()

        db = client.get_database(DB_NAME)
        collection = db[COLLECTION_NAME]

        collection.create_index(
            [("reviewId", pymongo.ASCENDING)],
            unique=True,
            background=True
        )

        csv_path = Path(CSV_PATH)
        if not csv_path.is_file():
            raise AirflowException(f"CSV не найден: {csv_path}")

        file_size = csv_path.stat().st_size
        logger.info(f"CSV: {csv_path} (размер {file_size:,} байт)")
        logger.info(f"Чтение чанками по {BATCH_SIZE:,} строк (без лимита)")

        total_processed = 0
        total_upserted = total_matched = total_modified = 0

        reader = pd.read_csv(
            csv_path,
            dtype=str,
            encoding="utf-8",
            on_bad_lines="warn",
            low_memory=True,
            chunksize=BATCH_SIZE
        )

        unique_field = "reviewId"

        for chunk_number, chunk_df in enumerate(reader, start=1):
            if chunk_df.empty:
                continue

            logger.info(f"Чанк #{chunk_number}: {len(chunk_df):,} строк")

            records = chunk_df.to_dict("records")

            if unique_field not in chunk_df.columns:
                logger.warning(
                    f"Колонка '{unique_field}' отсутствует → используем ReplaceOne"
                )
                operations = [
                    pymongo.ReplaceOne(
                        {"_id": str(hash(tuple(rec.values())))},
                        rec,
                        upsert=True
                    )
                    for rec in records
                ]
            else:
                operations = [
                    pymongo.UpdateOne(
                        {unique_field: rec.get(unique_field)},
                        {"$set": rec},
                        upsert=True
                    )
                    for rec in records
                ]

            if operations:
                result = collection.bulk_write(operations, ordered=False)

                total_processed += len(records)
                total_upserted += result.upserted_count
                total_matched += result.matched_count
                total_modified += result.modified_count

                logger.info(
                    f"Чанк завершён | "
                    f"Upserted: {result.upserted_count:,} | "
                    f"Modified: {result.modified_count:,} | "
                    f"Matched: {result.matched_count:,}"
                )

        logger.info("Загрузка завершена")
        logger.info(
            f"Всего строк: {total_processed:,} | "
            f"Новых: {total_upserted:,} | "
            f"Обновлено: {total_modified:,} | "
            f"Без изменений: {total_matched:,}"
        )

    except pymongo.errors.BulkWriteError as bwe:
        logger.error("BulkWriteError", exc_info=True)
        raise AirflowException(bwe.details)
    except pymongo.errors.PyMongoError as e:
        logger.error("Mongo ошибка", exc_info=True)
        raise AirflowException(str(e))
    except Exception as e:
        logger.error("Общая ошибка", exc_info=True)
        raise AirflowException(str(e))
    finally:
        if client:
            client.close()
            logger.info("Mongo клиент закрыт")


with DAG(
    dag_id="load_to_mongo",
    description="Загрузка CSV целиком в MongoDB",
    start_date=datetime(2026, 1, 1),
    schedule=[processed_reviews_dataset],
    catchup=False,
    tags=["mongo", "dataset", "etl"],
    max_active_runs=1,
) as dag:

    load_task = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_to_mongo,
        inlets=[processed_reviews_dataset],
    )

    load_task
