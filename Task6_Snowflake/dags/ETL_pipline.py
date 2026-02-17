from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import sys
import os

# Добавляем путь к скриптам
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
from load_raw_data import main as run_ingestion

default_args = {
    'owner': 'airflow',
}

with DAG(
    'airline_dwh_pipeline',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,          
    catchup=False
) as dag:

    ingest_data = PythonOperator(
        task_id='ingest_csv_to_snowflake',
        python_callable=run_ingestion
    )

    load_dims = SQLExecuteQueryOperator(
        task_id='load_dimensions',
        sql="CALL AIRLINE_DB.HARMONIZED.LOAD_DIMENSIONS();",
        conn_id='snowflake_default'
    )

    load_facts = SQLExecuteQueryOperator(
        task_id='load_facts',
        sql="CALL AIRLINE_DB.ANALYTICS.LOAD_FACT_BOOKINGS();",
        conn_id='snowflake_default'
    )

    ingest_data >> load_dims >> load_facts