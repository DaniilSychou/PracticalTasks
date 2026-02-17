import os
import logging
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from dotenv import load_dotenv


# --- КОНФИГУРАЦИЯ ---
load_dotenv()

LOCAL_FILE_PATH = os.getenv("LOCAL_FILE_PATH")
STAGE_NAME = os.getenv("STAGE_NAME")
LOG_DIR = os.getenv("LOG_DIR", "py_logs")

def setup_logger():
    """Настройка логгера для записи в файл и консоль"""
    # Создаем папку, если она не существует
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        print(f"Создана папка для логов: {os.path.abspath(LOG_DIR)}")

    # Формируем имя файла с текущей датой
    log_filename = f"snowflake_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = os.path.join(LOG_DIR, log_filename)

    # Настройка
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'), 
            logging.StreamHandler()                          
        ]
    )
    return log_path

def main():
    # Инициализация логгера
    log_file = setup_logger()
    logging.info(f"Логирование настроено. Файл лога: {log_file}")

    if not os.path.isfile(LOCAL_FILE_PATH):
        error_msg = f"CSV-файл не найден по пути: {LOCAL_FILE_PATH}"
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)

    logging.info(f"Запуск загрузки через SnowflakeHook → {LOCAL_FILE_PATH}")

    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    
    try:
        cur = conn.cursor()

        # ШАГ 1: PUT (загружаем файл на stage таблицы)
        abs_path = os.path.abspath(LOCAL_FILE_PATH)
        put_sql = f"PUT file://{abs_path} {STAGE_NAME} AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
        
        logging.info(f"→ Выполнение PUT: {put_sql}")
        cur.execute(put_sql)
        
        # Получаем результат PUT для лога
        put_result = cur.fetchall()
        logging.info(f"PUT завершен. Результат: {put_result}")

        # ШАГ 2: COPY INTO
        copy_sql = f"""
        COPY INTO STG_FLIGHTS_PASSENGERS_RAW (
            PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, NATIONALITY, 
            AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, 
            CONTINENTS, DEPARTURE_DATE, ARRIVAL_AIRPORT, PILOT_NAME, 
            FLIGHT_STATUS, TICKET_TYPE, PASSENGER_STATUS
        )
        FROM {STAGE_NAME}/airline_dataset.csv
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            TRIM_SPACE = TRUE
            DATE_FORMAT = 'MM/DD/YYYY'
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        )
        ON_ERROR = 'CONTINUE'
        PURGE = TRUE
        FORCE = TRUE
        """
        logging.info("→ Выполнение COPY INTO (FORCE=TRUE)...")
        cur.execute(copy_sql)
        
        copy_result = cur.fetchall()
        logging.info(f"COPY результат: {copy_result}")


        cur.execute("SELECT COUNT(*) FROM STG_FLIGHTS_PASSENGERS_RAW")
        count = cur.fetchone()[0]
        logging.info(f"Загрузка успешно завершена. Всего строк в STG: {count}")

    except Exception as e:
        # exc_info=True добавит полный Traceback ошибки в лог
        logging.error(f"Критическая ошибка при загрузке: {e}", exc_info=True)
        raise
    finally:
        cur.close()
        conn.close()
        logging.info("Соединение со Snowflake закрыто.")

if __name__ == "__main__":
    main()