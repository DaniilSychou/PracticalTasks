from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_mongo():
    hook = MongoHook(mongo_conn_id='mongo_oconnect')  # или твое имя
    client = hook.get_conn()
    db = client.get_database('textdb')           # или твоя база
    print(list(db.list_collection_names()))          # выведет коллекции

with DAG(
    dag_id='mongo_oconnect',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    task = PythonOperator(task_id='test_conn', python_callable=test_mongo)