select ci.city,
       SUM(CASE WHEN c.active = 1 THEN 1 ELSE 0 END) AS active_customer,
       SUM(CASE WHEN c.active = 0 THEN 1 ELSE 0 END) AS not_active_customer
from city ci
join address a ON ci.city_id = a.city_id
join customer c ON a.address_id = c.address_id
GROUP BY ci.city
ORDER BY not_active_customer DESC;
