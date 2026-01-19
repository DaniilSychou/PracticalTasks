FROM apache/spark:4.0.1

# Переключаемся на root, чтобы установить системные пакеты
USER root

# Устанавливаем системные зависимости для Python и PostgreSQL
# Обратите внимание: в новых образах spark может потребоваться python3-full или venv
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    python3 \
    python3-pip \
    libpq-dev \
    gcc \
    python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /app

# Сначала копируем только requirements.txt (если он есть) для кэширования слоев
# Если его нет, библиотеки установятся из списка ниже
COPY requirements.txt . 

# Устанавливаем библиотеки Python. 
# Используем --break-system-packages, если в образе Spark включена защита Python окружения
RUN pip3 install --no-cache-dir \
    pyspark==4.0.1 \
    psycopg2-binary \
    python-dotenv

# Загружаем PostgreSQL JDBC драйвер
# Он необходим для быстрой работы Spark с базой данных (метод .write.jdbc)
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o /opt/spark/jars/postgresql-42.7.3.jar

# Копируем все файлы вашего проекта в контейнер
COPY . /app

# Устанавливаем правильные права (Spark часто запускается от пользователя spark с UID 185)
RUN chown -R 185:185 /app

# Переключаемся обратно на стандартного пользователя Spark для безопасности
USER 185

# Запускаем скрипт через python3
CMD ["python3", "main.py"]