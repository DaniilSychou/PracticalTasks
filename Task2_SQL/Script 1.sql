select count(film_id) count_films, c.name 	
	from public.film_category fc
	join public.category c on c.category_id = fc. category_id 
	group by c.name 
	order by count_films desc

