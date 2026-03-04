INSERT INTO core.dim_ship_mode (ship_mode)
SELECT DISTINCT ship_mode 
FROM stage.superstore_raw 
WHERE ship_mode IS NOT NULL
ON CONFLICT (ship_mode) DO NOTHING;