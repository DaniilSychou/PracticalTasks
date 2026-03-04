FROM apache/airflow:3.1.6

# Обязательные пакеты для твоих DAG
RUN pip install --no-cache-dir \
    "redis>=4.5.2,<5.0.0" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-common-sql \
    apache-airflow-providers-standard \
    pandas \
    sqlalchemy \
    psycopg[binary] \
    python-dotenv

# Возвращаемся к airflow user
USER airflow