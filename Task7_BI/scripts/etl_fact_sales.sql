INSERT INTO core.fact_sales (
    order_id, order_date_key, ship_date_key, customer_key, 
    product_key, ship_mode_key, sales, quantity, discount, profit
)
SELECT
    s.order_id,
    od.date_key,
    sd.date_key,
    c.customer_key,
    p.product_key,
    sm.ship_mode_key,
    s.sales, s.quantity, s.discount, s.profit
FROM stage.superstore_raw s
JOIN core.dim_date od ON s.order_date = od.full_date
JOIN core.dim_date sd ON s.ship_date = sd.full_date
JOIN core.dim_product p ON s.product_id = p.product_id
JOIN core.dim_customer c ON s.customer_id = c.customer_id AND c.is_current = TRUE
JOIN core.dim_ship_mode sm ON s.ship_mode = sm.ship_mode
ON CONFLICT (order_id, product_key) DO NOTHING;  -- дедупликация