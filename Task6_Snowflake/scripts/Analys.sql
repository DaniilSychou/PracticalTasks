CREATE OR REPLACE PROCEDURE AIRLINE_DB.ANALYTICS.LOAD_FACT_BOOKINGS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    inserted_rows INT;
BEGIN
    -- 1. Начинаем транзакцию
    BEGIN TRANSACTION;

    -- 2. Очищаем таблицу фактов перед полной перезагрузкой
    -- (Если вы хотите инкрементальную загрузку, TRUNCATE нужно убрать)
    TRUNCATE TABLE ANALYTICS.FACT_FLIGHT_BOOKINGS;

    -- 3. Вставка данных с объединением всех измерений
    INSERT INTO ANALYTICS.FACT_FLIGHT_BOOKINGS (
        PASSENGER_SK, 
        DEPARTURE_AIRPORT_SK, 
        ARRIVAL_AIRPORT_SK, 
        PILOT_SK, 
        DEPARTURE_DATE, 
        FLIGHT_STATUS, 
        TICKET_TYPE, 
        PASSENGER_STATUS, 
        IS_DELAYED
    )
    SELECT 
        p.PASSENGER_SK,
        da.AIRPORT_SK,
        aa.AIRPORT_SK,
        pi.PILOT_SK,
        stg.DEPARTURE_DATE,
        stg.FLIGHT_STATUS,
        stg.TICKET_TYPE,
        stg.PASSENGER_STATUS,
        CASE WHEN stg.FLIGHT_STATUS = 'Delayed' THEN TRUE ELSE FALSE END
    FROM STG.STG_FLIGHTS_PASSENGERS_RAW stg
    -- Используем INNER JOIN, чтобы гарантировать целостность (строка попадет, только если есть во всех DIM)
    JOIN HARMONIZED.DIM_PASSENGER p ON stg.PASSENGER_ID = p.PASSENGER_ID
    JOIN HARMONIZED.DIM_AIRPORT da ON stg.AIRPORT_NAME = da.AIRPORT_NAME
    JOIN HARMONIZED.DIM_AIRPORT aa ON stg.ARRIVAL_AIRPORT = aa.AIRPORT_NAME
    JOIN HARMONIZED.DIM_PILOT pi ON stg.PILOT_NAME = pi.PILOT_NAME;

    inserted_rows := SQLROWCOUNT;

    -- 4. Запись в аудит
    INSERT INTO AUDIT.AUDIT_LOAD_LOG (run_id, process_name, target_table, inserted_rows, status)
    VALUES (UUID_STRING(), 'ANALYTICS LOAD', 'FACT_FLIGHT_BOOKINGS', :inserted_rows, 'SUCCESS');

    -- 5. Подтверждаем транзакцию
    COMMIT;

    RETURN 'Fact table loaded successfully. Rows: ' || :inserted_rows;

EXCEPTION
    -- Если произошла ошибка - откатываем изменения
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'Error in LOAD_FACT_BOOKINGS: ' || SQLERRM;
END;
$$;