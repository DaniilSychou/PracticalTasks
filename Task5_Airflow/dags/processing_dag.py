import os
import re
import logging
import sys
from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Пути (все промежуточные файлы в одной папке)
# ──────────────────────────────────────────────────────────────

DATA_DIR = "/opt/airflow/data"
INPUT_FILE = os.path.join(DATA_DIR, "tiktok_google_play_reviews.csv")
TEMP_FILE_AFTER_LOAD = os.path.join(DATA_DIR, "temp_after_load.parquet")
TEMP_FILE_AFTER_REPLACE = os.path.join(DATA_DIR, "temp_after_replace.parquet")
TEMP_FILE_AFTER_SORT = os.path.join(DATA_DIR, "temp_after_sort.parquet")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed_tiktok_reviews.csv")

processed_reviews_dataset = Dataset(PROCESSED_FILE)

default_args = {
    "owner": "danil",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="tiktok_google_play_reviews_processing",
    default_args=default_args,
    description="Обработка отзывов TikTok → создание processed файла",
    schedule=None,
    catchup=False,
    tags=["tiktok", "etl", "csv", "dataset"],
    max_active_runs=1,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_input_file",
        filepath=INPUT_FILE,
        poke_interval=30,
        timeout=7200,
        mode="poke",
    )

    def check_if_file_empty(**context):
        if not os.path.exists(INPUT_FILE):
            logger.error(f"Файл не найден: {INPUT_FILE}")
            return "log_empty_file"

        size = os.path.getsize(INPUT_FILE)
        logger.info(f"Размер: {size:,} байт")

        return "process_data_group" if size > 0 else "log_empty_file"

    branch_check = BranchPythonOperator(
        task_id="branch_check_file_empty",
        python_callable=check_if_file_empty,
    )

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command=f'echo "[$(date)] Файл пустой: {INPUT_FILE}" >> {DATA_DIR}/empty_files.log',
    )

    with TaskGroup(group_id="process_data_group") as process_group:

        def load_data(**context):
            logger.info("===== load_data START =====")
            logger.info(f"Путь: {INPUT_FILE}")
            logger.info(f"size: {os.path.getsize(INPUT_FILE):,} байт")

            try:
                df = pd.read_csv(
                    INPUT_FILE,
                    dtype=str,
                    encoding='utf-8',
                    low_memory=True,
                    on_bad_lines='warn'
                )
                logger.info(f"Прочитано строк: {len(df):,}")
                logger.info(f"Колонки: {df.columns.tolist()}")

                if df.empty:
                    raise AirflowException("CSV пустой")

                # Сохраняем в parquet (быстрее и меньше памяти, чем csv)
                df.to_parquet(TEMP_FILE_AFTER_LOAD, index=False, engine='pyarrow')
                logger.info(f"Сохранено в {TEMP_FILE_AFTER_LOAD}")

                # Push пути
                context["ti"].xcom_push(key="temp_file", value=TEMP_FILE_AFTER_LOAD)
                logger.info(f"XCom push key='temp_file' выполнен, значение: {TEMP_FILE_AFTER_LOAD}")

                # Тестовый push строки
                context["ti"].xcom_push(key="test_key", value="TEST_FROM_LOAD_DATA")
                logger.info("Тестовый XCom push 'test_key' выполнен")

            except Exception as e:
                logger.error("Ошибка в load_data", exc_info=True)
                raise AirflowException(f"Ошибка чтения/сохранения: {e}")

        def replace_nulls(**context):
            temp_file = TEMP_FILE_AFTER_LOAD  # фиксированный путь
            logger.info(f"replace_nulls: чтение из фиксированного пути {temp_file}")

            if not os.path.exists(temp_file):
                raise AirflowException(f"Файл {temp_file} не найден после load_data")

            df = pd.read_parquet(temp_file, engine='pyarrow')
            logger.info(f"Строк: {len(df):,}")

            df = df.fillna("-")
            df.to_parquet(TEMP_FILE_AFTER_REPLACE, index=False, engine='pyarrow')
            logger.info(f"Сохранено в {TEMP_FILE_AFTER_REPLACE}")

        def sort_by_date(**context):
            temp_file = TEMP_FILE_AFTER_REPLACE
            logger.info(f"sort_by_date: чтение из {temp_file}")

            df = pd.read_parquet(temp_file, engine='pyarrow')

            date_cols = ["at", "created_at", "timestamp", "date", "review_created_at"]
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    df = df.sort_values(col, ascending=False)
                    logger.info(f"Отсортировано по {col}")
                    break

            df.to_parquet(TEMP_FILE_AFTER_SORT, index=False, engine='pyarrow')
            logger.info(f"Сохранено в {TEMP_FILE_AFTER_SORT}")

        def clean_and_save(**context):
            temp_file = TEMP_FILE_AFTER_SORT
            logger.info(f"clean_and_save: чтение из {temp_file}")

            df = pd.read_parquet(temp_file, engine='pyarrow')

            if "content" in df.columns:
                df["content"] = df["content"].astype(str).apply(
                    lambda x: re.sub(r"[^\w\s\.,!?]", "", x)
                )

            df.to_csv(PROCESSED_FILE, index=False, encoding="utf-8-sig")
            logger.info(f"Готовый файл сохранён: {PROCESSED_FILE}")

        load = PythonOperator(task_id="load_data", python_callable=load_data)
        replace = PythonOperator(task_id="replace_nulls", python_callable=replace_nulls)
        sort = PythonOperator(task_id="sort_by_date", python_callable=sort_by_date)
        save = PythonOperator(
            task_id="clean_and_save",
            python_callable=clean_and_save,
            outlets=[processed_reviews_dataset],
        )

        load >> replace >> sort >> save

    wait_for_file >> branch_check
    branch_check >> [log_empty_file, process_group]