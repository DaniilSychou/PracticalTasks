-- master_initial.sql
\echo '=== INITIAL LOAD MODE ==='

-- Создаём схемы и таблицы
\i ddl.sql
-- Наполняем dim_date (только один раз!)
\i populate_dim_date.sql

-- Загружаем данные
\set input_file '/opt/airflow/data/superstore_initial.csv'
\i load_data.sql

-- Остальные шаги
\i load_dim_ship_mode.sql
\i SCD1.sql
\i SCD2.sql
\i etl_fact_sales.sql
\i update_mart.sql

SELECT '=== INITIAL PIPELINE COMPLETED ===' AS status;