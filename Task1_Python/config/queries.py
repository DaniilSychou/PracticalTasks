
QUERIES = {
    "Select_count_rooms": """
        SELECT r.name, COUNT(s.room_id) AS student_count
        FROM {schema}.rooms r
        LEFT JOIN {schema}.students s ON r.rooms_id = s.room_id
        GROUP BY r.name
        ORDER BY student_count DESC;
    """,
    "Select_low_5ages": """
        SELECT r.name, ROUND(AVG(EXTRACT(YEAR FROM age(current_date, s.birthday)))) AS avg_age
        FROM {schema}.rooms r
        INNER JOIN {schema}.students s ON r.rooms_id = s.room_id
        GROUP BY r.name
        ORDER BY avg_age ASC
        LIMIT 5;
    """,
    "Select_high_between_ages": """
        SELECT r.name,
               MAX(ROUND(EXTRACT(YEAR FROM age(current_date, s.birthday))))
               - MIN(ROUND(EXTRACT(YEAR FROM age(current_date, s.birthday)))) AS age_range
        FROM {schema}.rooms r
        INNER JOIN {schema}.students s ON r.rooms_id = s.room_id
        GROUP BY r.name
        ORDER BY age_range DESC
        LIMIT 5;
    """,
    "Select_genders": """
        SELECT r.rooms_id, r.name
        FROM {schema}.rooms r
        JOIN {schema}.students s ON s.room_id = r.rooms_id
        GROUP BY r.rooms_id, r.name
        HAVING COUNT(DISTINCT s.sex) > 1;
    """
}