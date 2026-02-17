FROM apache/airflow:3.1.6

# Переключаемся на пользователя airflow (важно!)
USER airflow

# Копируем requirements.txt и устанавливаем пакеты
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Возвращаем пользователя root (если нужно для других задач)
USER root