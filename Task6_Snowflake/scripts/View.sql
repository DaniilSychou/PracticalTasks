USE ROLE ACCOUNTADMIN;
USE DATABASE AIRLINE_DB;
USE SCHEMA ANALYTICS;

-- =========================================================
-- ШАГ 1: ОТВЯЗЫВАЕМ ПОЛИТИКУ (Сброс)
-- =========================================================
-- Мы удаляем представления, к которым может быть привязана политика.
-- Это освободит политику для редактирования.

DROP VIEW IF EXISTS ANALYTICS.V_FACT_SECURE;
DROP VIEW IF EXISTS ANALYTICS.V_FACT_BOOKINGS_SECURE;

-- Если вы случайно привязали политику к самой таблице, выполните это:
-- ALTER TABLE IF EXISTS ANALYTICS.FACT DROP ROW ACCESS POLICY SECURITY.TICKET_TYPE_POLICY;


-- =========================================================
-- ШАГ 2: ПЕРЕСОЗДАЕМ ПОЛИТИКУ
-- =========================================================
-- Теперь, когда политика нигде не используется, её можно заменить.

CREATE OR REPLACE ROW ACCESS POLICY SECURITY.TICKET_TYPE_POLICY 
AS (ticket_val VARCHAR) RETURNS BOOLEAN ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN TRUE
        ELSE EXISTS (
            SELECT 1 FROM SECURITY.ACCESS_RULES
            WHERE ROLE_NAME = CURRENT_ROLE()
              -- Сравнение без учета регистра
              AND UPPER(ALLOWED_TICKET_TYPE) = UPPER(ticket_val)
        )
    END;


-- =========================================================
-- ШАГ 3: ОБНОВЛЯЕМ ПРАВИЛА (Если нужно)
-- =========================================================
CREATE TABLE IF NOT EXISTS SECURITY.ACCESS_RULES (
    ROLE_NAME VARCHAR,
    ALLOWED_TICKET_TYPE VARCHAR
);

-- Очистим старые правила, чтобы не путаться
TRUNCATE TABLE SECURITY.ACCESS_RULES;

-- Добавляем правило для Business
INSERT INTO SECURITY.ACCESS_RULES (ROLE_NAME, ALLOWED_TICKET_TYPE)
VALUES ('BUSINESS_MANAGER_ROLE', 'Business');


-- =========================================================
-- ШАГ 4: СОЗДАЕМ VIEW ЗАНОВО (Привязываем обновленную политику)
-- =========================================================
-- Используем вашу таблицу ANALYTICS.FACT 
-- Убедитесь, что в ней есть колонка TICKET_TYPE

CREATE OR REPLACE SECURE VIEW ANALYTICS.V_FACT_SECURE
WITH ROW ACCESS POLICY SECURITY.TICKET_TYPE_POLICY ON (TICKET_TYPE)
AS 
SELECT * 
FROM ANALYTICS.FACT_FLIGHT_BOOKINGS;

-- =========================================================
-- ШАГ 5: ВЫДАЕМ ПРАВА И ПРОВЕРЯЕМ
-- =========================================================

-- Убедимся, что роль существует
CREATE OR REPLACE ROLE BUSINESS_MANAGER_ROLE;
GRANT ROLE BUSINESS_MANAGER_ROLE TO ROLE SYSADMIN;

-- Выдаем доступы
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE BUSINESS_MANAGER_ROLE;
GRANT USAGE ON DATABASE AIRLINE_DB TO ROLE BUSINESS_MANAGER_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE BUSINESS_MANAGER_ROLE;
GRANT SELECT ON VIEW ANALYTICS.V_FACT_SECURE TO ROLE BUSINESS_MANAGER_ROLE;

-- ПРОВЕРКА
USE ROLE BUSINESS_MANAGER_ROLE;
SELECT DISTINCT TICKET_TYPE FROM ANALYTICS.V_FACT_SECURE;
-- Должен вернуться только Business