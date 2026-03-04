import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk.definitions.template import literal
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from airflow.models import Variable


load_dotenv()

PROJECT_ROOT = os.getenv("SUPERSTORE_PROJECT_ROOT", "/opt/airflow/scripts") 
DATA_DIR = os.getenv("SUPERSTORE_OUTPUT_DIR",       "/opt/airflow/data")
INITIAL_CSV = os.getenv("SUPERSTORE_INITIAL_CSV",      f"{DATA_DIR}/superstore_initial.csv")
SECONDARY_CSV = os.getenv("SUPERSTORE_SECONDARY_CSV", f"{DATA_DIR}/superstore_secondary.csv")
DB_USER = os.getenv("DB_USER", "postgres")
DB_NAME = os.getenv("DB_NAME", "superstore")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_PASSWORD = os.getenv("DB_PASS", "AsSQL")

PSQL_CMD = (
    f"PGPASSWORD={DB_PASSWORD} "
    f"psql -h {DB_HOST} -p {DB_PORT} -U {DB_USER} -d {DB_NAME} -v ON_ERROR_STOP=1"
)

default_args = {
    'owner': 'bi_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='superstore_bi_full_etl_pipeline',
    default_args=default_args,
    description='Superstore BI pipeline: data split → initial load → incremental load',
    schedule=None,                  # запускаем вручную / через trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['superstore', 'etl', 'bi', 'powerbi', 'scd'],
    max_active_runs=1,
) as dag:

    split_task = PythonOperator(
        task_id='split_dataset',
        python_callable=lambda: __import__('data_splitter').split_csv(),
    )

    initial_etl_steps = BashOperator(
        task_id='run_initial_etl_steps',
        bash_command=f"""
            set -e
            # Сначала переходим в папку, чтобы psql видел соседние файлы (ddl.sql)
            cd {PROJECT_ROOT}
            
            # Запускаем psql прямо из текущей папки
            {PSQL_CMD} -v mode=initial -f master_pipeline.sql
        """,
    )

    incremental_etl_steps = BashOperator(
        task_id='run_incremental_etl_steps',
        bash_command=f"""
            set -e
            # Тут тоже переходим
            cd {PROJECT_ROOT}
            
            {PSQL_CMD} -v mode=secondary -f master_pipeline.sql
        """,
    )

    split_task >> initial_etl_steps >> incremental_etl_steps