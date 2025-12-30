select a.first_name,                  
    a.last_name,                        
    count(r.rental_id) AS rental_count  
from rental r                         
join inventory i ON r.inventory_id = i.inventory_id  
join film f ON i.film_id = f.film_id                
join film_actor fa ON f.film_id = fa.film_id         
join actor a ON fa.actor_id = a.actor_id            
GROUP BY a.actor_id, a.first_name, a.last_name          
ORDER BY rental_count DESC                               
LIMIT 10;                                           
