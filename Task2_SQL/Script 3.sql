select c.name AS category_name,                        
    SUM(p.amount) AS total_revenue                  
from payment p                                
join rental r  USING(rental_id)   
join inventory i  USING(inventory_id)  
join film_category fc  USING(film_id)
join category c USING(category_id) 
GROUP BY c.name                                      
ORDER BY total_revenue DESC                             
LIMIT 1;                                   