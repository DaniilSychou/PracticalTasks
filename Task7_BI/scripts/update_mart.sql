
INSERT INTO mart.dim_date SELECT * FROM core.dim_date;
INSERT INTO mart.dim_customer SELECT * FROM core.dim_customer;
INSERT INTO mart.dim_product SELECT * FROM core.dim_product;
-- Наполняем витрину из уже обработанного Core-слоя
INSERT INTO mart.fact_sales
SELECT
    f.sales_key,
    f.order_id,
    od.full_date AS order_date,
    c.customer_name,
    c.segment,
    p.product_name,
    f.sales,
    f.quantity,
    f.profit
FROM core.fact_sales f
JOIN core.dim_date od ON f.order_date_key = od.date_key
JOIN core.dim_customer c ON f.customer_key = c.customer_key
JOIN core.dim_product p ON f.product_key = p.product_key
WHERE c.is_current = TRUE; -- Берем только актуальные версии клиентов