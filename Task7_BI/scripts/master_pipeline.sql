-- master_pipeline.sql
-- Универсальный мастер-файл с условной логикой (initial / incremental)

-- 1. Проверка, что переменная mode передана
\if :'mode' = ''
    \echo 'Ошибка: переменная mode не задана! Используйте -v mode=initial или -v mode=incremental'
    \q
\endif

-- 2. Вычисляем булевый флаг
SELECT 
    CASE WHEN :'mode' ILIKE 'initial' THEN 'true' ELSE 'false' END AS is_initial
\gset

-- 3. Условная часть
\if :is_initial
    \echo '=== INITIAL LOAD MODE ==='
    \i ddl.sql
    \i populate_dim_date.sql
\else
    \echo '=== INCREMENTAL MODE ==='
\endif

-- 4. Определяем имя файла в зависимости от режима
\set input_file '/opt/airflow/data/superstore_':mode'.csv'

-- 5. Выводим для отладки, какой файл будем загружать
\echo 'Загружаемый файл: ' :'input_file'

-- 6. Общая загрузка stage-данных (используем \copy — клиентский вариант)
\i load_data.sql
SELECT '=== DATA LOADING COMPLETED ===';
-- 7. Остальные шаги ETL
\i load_dim_ship_mode.sql
\i SCD1.sql
\i SCD2.sql
SELECT '=== SCD COMPLETED ===';
\i etl_fact_sales.sql
SELECT '=== FACT SALES ETL COMPLETED ===';
\i update_mart.sql
SELECT '=== MART UPDATE COMPLETED ===';
-- 8. Финальный статус
SELECT '=== PIPELINE COMPLETED IN MODE: ' || :'mode' || ' ===' AS status;