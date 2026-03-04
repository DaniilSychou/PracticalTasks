TRUNCATE TABLE stage.superstore_raw;

COPY stage.superstore_raw (
    row_id, order_id, order_date, ship_date, ship_mode,
    customer_id, customer_name, segment,
    country, city, state, postal_code, region,
    product_id, category, subcategory, product_name,
    sales, quantity, discount, profit
)
FROM :'input_file'
WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'UTF8');