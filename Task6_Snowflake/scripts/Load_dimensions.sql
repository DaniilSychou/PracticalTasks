CREATE OR REPLACE PROCEDURE AIRLINE_DB.HARMONIZED.LOAD_DIMENSIONS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    -- 1. Явно открываем транзакцию
    BEGIN TRANSACTION;

    -- 2. Замораживаем данные из стрима во временную таблицу
    -- Это предотвращает проблемы с "потреблением" стрима в нескольких инсертах
    CREATE OR REPLACE TEMPORARY TABLE HARMONIZED.TEMP_STG_DATA AS 
    SELECT * FROM STG.STG_FLIGHTS_STREAM;

    -- 3. Если данных нет - просто выходим и закрываем транзакцию
    IF ((SELECT COUNT(*) FROM HARMONIZED.TEMP_STG_DATA) = 0) THEN
        COMMIT;
        RETURN 'No new data in stream.';
    END IF;

    -- 4. Загружаем Пилотов
    INSERT INTO HARMONIZED.DIM_PILOT (PILOT_NAME)
    SELECT DISTINCT PILOT_NAME FROM HARMONIZED.TEMP_STG_DATA
    WHERE PILOT_NAME NOT IN (SELECT PILOT_NAME FROM HARMONIZED.DIM_PILOT);
    
    -- 5. Загружаем Аэропорты (имена вылета и прилета)
    INSERT INTO HARMONIZED.DIM_AIRPORT (AIRPORT_NAME)
    SELECT DISTINCT AIRPORT_NAME FROM (
        SELECT AIRPORT_NAME FROM HARMONIZED.TEMP_STG_DATA
        UNION 
        SELECT ARRIVAL_AIRPORT FROM HARMONIZED.TEMP_STG_DATA
    ) AS all_airports
    WHERE AIRPORT_NAME NOT IN (SELECT AIRPORT_NAME FROM HARMONIZED.DIM_AIRPORT);

    -- 6. Загружаем Пассажиров
    INSERT INTO HARMONIZED.DIM_PASSENGER (PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, NATIONALITY, PASSENGER_STATUS)
    SELECT DISTINCT PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, NATIONALITY, PASSENGER_STATUS
    FROM HARMONIZED.TEMP_STG_DATA
    WHERE PASSENGER_ID NOT IN (SELECT PASSENGER_ID FROM HARMONIZED.DIM_PASSENGER);

    -- 7. Пишем в лог аудита
    INSERT INTO AUDIT.AUDIT_LOAD_LOG (run_id, process_name, target_table, inserted_rows, status)
    VALUES (UUID_STRING(), 'HARMONIZED LOAD', 'DIMENSIONS', (SELECT COUNT(*) FROM HARMONIZED.TEMP_STG_DATA), 'SUCCESS');

    -- 8. Если всё прошло успешно - фиксируем изменения
    COMMIT;
    
    RETURN 'Dimensions loaded successfully. Rows processed: ' || (SELECT COUNT(*) FROM HARMONIZED.TEMP_STG_DATA);

EXCEPTION
    -- Если что-то пошло не так - откатываем всё назад
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'Error occurred: ' || SQLERRM;
END;
$$;