-- SCD1.sql
-- Обработка dim_product — SCD Type 1 (перезапись атрибутов)

-- 1. Вставка только новых продуктов
INSERT INTO core.dim_product (
    product_id, category, subcategory, product_name
)
SELECT DISTINCT 
    s.product_id, 
    s.category, 
    s.subcategory, 
    s.product_name
FROM stage.superstore_raw s
WHERE NOT EXISTS (
    SELECT 1 
    FROM core.dim_product p 
    WHERE p.product_id = s.product_id
)
ON CONFLICT (product_id) DO NOTHING;

-- 2. Обновление атрибутов у существующих продуктов (если что-то изменилось)
UPDATE core.dim_product p
SET 
    category     = src.category,
    subcategory  = src.subcategory,
    product_name = src.product_name
FROM (
    SELECT DISTINCT 
        product_id, 
        category, 
        subcategory, 
        product_name
    FROM stage.superstore_raw
) src
WHERE p.product_id = src.product_id
  AND (
      p.category     IS DISTINCT FROM src.category     OR
      p.subcategory  IS DISTINCT FROM src.subcategory  OR
      p.product_name IS DISTINCT FROM src.product_name
  );