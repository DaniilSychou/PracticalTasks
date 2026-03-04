-- SCD2.sql
-- Обработка dim_customer — SCD Type 2 (сохранение истории изменений)

-- 1. Закрываем текущую версию, если изменился хотя бы один атрибут SCD Type 2
UPDATE core.dim_customer c
SET 
    valid_to   = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
FROM stage.superstore_raw s
WHERE c.customer_id = s.customer_id
  AND c.is_current = TRUE
  AND (
      c.customer_name  IS DISTINCT FROM s.customer_name  OR
      c.segment        IS DISTINCT FROM s.segment        OR
      c.country        IS DISTINCT FROM s.country        OR
      c.city           IS DISTINCT FROM s.city           OR
      c.state          IS DISTINCT FROM s.state           OR
      c.postal_code    IS DISTINCT FROM s.postal_code    OR
      c.region         IS DISTINCT FROM s.region
  );

-- 2. Вставляем новую версию только если сейчас нет активной записи
--    + защита от дубликатов в один день
INSERT INTO core.dim_customer (
    customer_id, 
    customer_name, 
    segment, 
    country, 
    city, 
    state, 
    postal_code, 
    region,
    valid_from, 
    valid_to, 
    is_current
)
SELECT DISTINCT
    s.customer_id,
    s.customer_name,
    s.segment,
    s.country,
    s.city,
    s.state,
    s.postal_code,
    s.region,
    CURRENT_DATE           AS valid_from,
    NULL::date             AS valid_to,          -- явно приводим к типу date
    TRUE                   AS is_current
FROM stage.superstore_raw s
WHERE NOT EXISTS (
    SELECT 1 
    FROM core.dim_customer c 
    WHERE c.customer_id = s.customer_id 
      AND c.is_current = TRUE
)
ON CONFLICT (customer_id, valid_from) DO NOTHING;