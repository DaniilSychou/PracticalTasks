-- DML 1: Посмотреть данные в STG 5 минут назад
SELECT * FROM STG.STG_FLIGHTS_PASSENGERS_RAW AT(OFFSET => -300);

-- DML 2: Восстановить случайно удаленные факты (пример)
INSERT INTO ANALYTICS.FACT_FLIGHT_BOOKINGS 
SELECT * FROM ANALYTICS.FACT_FLIGHT_BOOKINGS BEFORE(STATEMENT => 'query_id_here');

-- DDL 1: Клонирование всей схемы стейджинга (Zero-copy clone)
CREATE SCHEMA STG.STG_COPY CLONE STG;

-- DDL 2: Восстановление удаленной таблицы
DROP TABLE ANALYTICS.FACT_FLIGHT_BOOKINGS;
UNDROP TABLE ANALYTICS.FACT_FLIGHT_BOOKINGS;
