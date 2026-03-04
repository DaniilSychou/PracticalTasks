INSERT INTO core.dim_date (date_key, full_date, year, quarter, month, month_name, day)
SELECT 
    CAST(to_char(d, 'YYYYMMDD') AS INT),
    d,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    TO_CHAR(d, 'Month'),
    EXTRACT(DAY FROM d)::INT
FROM generate_series('2014-01-01'::date, '2017-12-31'::date, '1 day') d
ON CONFLICT (date_key) DO NOTHING;