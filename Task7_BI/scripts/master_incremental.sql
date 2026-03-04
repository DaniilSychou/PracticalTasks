-- master_incremental.sql
\echo '=== INCREMENTAL MODE ==='

\set input_file '/opt/airflow/data/superstore_secondary.csv'   -- или superstore_incremental.csv — как назвал
\i load_data.sql

\i load_dim_ship_mode.sql
\i SCD1.sql
\i SCD2.sql
\i etl_fact_sales.sql
\i update_mart.sql

SELECT '=== INCREMENTAL PIPELINE COMPLETED ===' AS status;