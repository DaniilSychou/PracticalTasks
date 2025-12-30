WITH rental_hours AS (
    SELECT 
        ci.city,
        cat.name AS category,
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS total_hours
    FROM city ci
    JOIN address a USING (city_id)
    JOIN customer cu USING (address_id)
    JOIN rental r USING (customer_id)
    JOIN inventory i USING (inventory_id)
    JOIN film f USING (film_id)
    JOIN film_category fc USING (film_id)
    JOIN category cat USING (category_id)
    WHERE 
        ci.city ILIKE '%a%' 
        OR ci.city LIKE '%-%'
    GROUP BY ci.city, cat.name
),
ranked_categories AS (
    SELECT
        city,
        category,
        total_hours,
        DENSE_RANK() OVER (
            PARTITION BY city
            ORDER BY total_hours DESC
        ) AS rnk
    FROM rental_hours
)
SELECT
    city,
    category,
    ROUND(total_hours, 2) AS total_hours,
    CASE
        WHEN city LIKE '%-%' THEN 'city-with-dash'
        ELSE 'city-with-a'
    END AS city_type
FROM ranked_categories
WHERE rnk = 1
ORDER BY city;
