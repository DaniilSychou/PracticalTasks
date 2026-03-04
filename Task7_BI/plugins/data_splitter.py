from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import random
import os
from dotenv import load_dotenv
import logging

# Загружаем .env
load_dotenv()

# Настройка логгера
split_logger = logging.getLogger(__name__)
split_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
split_logger.addHandler(handler)

def split_csv() -> None:
    """
    Разделяет Superstore.csv на initial и secondary с имитацией:
    - дубликатов
    - SCD Type 1 (перезапись атрибутов)
    - SCD Type 2 (изменение города/региона → новая версия)
    - новых записей
    """
    # ─── Чтение переменных из .env ────────────────────────────────────────
    input_csv     = os.getenv("SUPERSTORE_INPUT_CSV")
    output_dir    = os.getenv("SUPERSTORE_OUTPUT_DIR")
    split_ratio   = float(os.getenv("SUPERSTORE_SPLIT_RATIO", 0.8))
    random_state  = int(os.getenv("SUPERSTORE_RANDOM_STATE", 42))

    if not input_csv:
        split_logger.error("SUPERSTORE_INPUT_CSV не задан в .env")
        raise ValueError("SUPERSTORE_INPUT_CSV не задан")

    input_path = Path(input_csv)
    if not input_path.is_file():
        split_logger.error(f"Файл не найден: {input_path}")
        raise FileNotFoundError(f"CSV-файл не найден: {input_path}")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    split_logger.info(f"Чтение файла: {input_path}")

    # Читаем с fallback на utf-8
    try:
        df = pd.read_csv(input_path, encoding="windows-1252", low_memory=False)
    except UnicodeDecodeError:
        split_logger.warning("windows-1252 не подошёл → пробуем utf-8")
        df = pd.read_csv(input_path, encoding="utf-8", low_memory=False)

    if df.empty:
        split_logger.error("Исходный CSV пуст")
        raise ValueError("Исходный CSV пуст")

    split_logger.info(f"Всего строк в исходном файле: {len(df):,}")

    random.seed(random_state)

    # ─── 1. Initial load ──────────────────────────────────────────────────
    initial_df = df.sample(frac=split_ratio, random_state=random_state)
    remaining_df = df.drop(initial_df.index)

    # ─── 2. Генерация secondary с изменениями ─────────────────────────────
    n_total_remaining = len(remaining_df)

    n_duplicates = int(n_total_remaining * 0.40)   # ~40% дубликатов
    n_scd1       = int(n_total_remaining * 0.20)   # ~20% SCD Type 1
    n_scd2       = int(n_total_remaining * 0.15)   # ~15% SCD Type 2
    n_new        = int(n_total_remaining * 0.25)   # ~25% новых записей

    # Дубликаты из initial
    duplicates = initial_df.sample(n=min(n_duplicates, len(initial_df)), random_state=random_state + 1).copy()

    # SCD Type 1 — лёгкие изменения (имя клиента, имя продукта)
    scd1 = remaining_df.sample(n=min(n_scd1, n_total_remaining), random_state=random_state + 2).copy()
    scd1['Customer Name'] = scd1['Customer Name'].apply(lambda x: x + " (corrected)" if random.random() < 0.6 else x)
    scd1['Product Name'] = scd1['Product Name'].str.replace(" ", "_", regex=False)

    # SCD Type 2 — изменение адреса (город, штат, почта, регион)
    scd2 = remaining_df.sample(n=min(n_scd2, n_total_remaining), random_state=random_state + 3).copy()
    cities  = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Seattle']
    states  = ['NY', 'CA', 'IL', 'TX', 'AZ', 'WA']
    regions = ['East', 'West', 'Central', 'South']
    scd2['City']        = np.random.choice(cities, size=len(scd2))
    scd2['State']       = np.random.choice(states, size=len(scd2))
    scd2['Postal Code'] = np.random.randint(10000, 99999, size=len(scd2)).astype(str)
    scd2['Region']      = np.random.choice(regions, size=len(scd2))

    # Новые записи — слегка модифицированные копии из initial
    new_rows = initial_df.sample(n=min(n_new, len(initial_df)), random_state=random_state + 4).copy()
    new_rows['Order ID'] = ['NEW-' + str(i) + '-' + datetime.now().strftime('%Y%m%d%H%M%S') for i in range(len(new_rows))]
    new_rows['Order Date'] = pd.to_datetime(new_rows['Order Date']) + pd.to_timedelta(np.random.randint(1, 180, len(new_rows)), unit='D')
    new_rows['Ship Date']  = new_rows['Order Date'] + pd.to_timedelta(np.random.randint(1, 10, len(new_rows)), unit='D')
    new_rows['Sales']  = (new_rows['Sales']  * np.random.uniform(0.85, 1.40, len(new_rows))).round(2)
    new_rows['Profit'] = (new_rows['Profit'] * np.random.uniform(0.70, 1.60, len(new_rows))).round(2)

    # Собираем secondary
    secondary_parts = [duplicates, scd1, scd2, new_rows]
    secondary_df = pd.concat(secondary_parts, ignore_index=True)

    # Перемешиваем, чтобы не было очевидных блоков
    secondary_df = secondary_df.sample(frac=1, random_state=random_state).reset_index(drop=True)

    # ─── Сохранение ───────────────────────────────────────────────────────
    initial_path   = output_path / "superstore_initial.csv"
    secondary_path = output_path / "superstore_secondary.csv"

    initial_df.to_csv(initial_path,   index=False, encoding="utf-8")
    secondary_df.to_csv(secondary_path, index=False, encoding="utf-8")

    split_logger.info(f"initial   → {len(initial_df):,} строк → {initial_path}")
    split_logger.info(f"secondary → {len(secondary_df):,} строк "
                      f"(dup:{len(duplicates)}, scd1:{len(scd1)}, scd2:{len(scd2)}, new:{len(new_rows)}) → {secondary_path}")
    split_logger.info("Разделение завершено успешно")


if __name__ == "__main__":
    try:
        split_csv()
    except Exception as e:
        split_logger.exception("Ошибка при разделении данных")
        raise