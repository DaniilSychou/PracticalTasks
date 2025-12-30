from main import db_conn


QUERIES = {
    "Select_count_rooms": f"""
        SELECT r.name, COUNT(s.room_id) AS student_count
        FROM {db.schema}.rooms r
        LEFT JOIN {schem_name}.students s ON r.rooms_id = s.room_id
        GROUP BY r.name
        ORDER BY student_count DESC;
    """,
    "Select_low_5ages": f"""
        SELECT r.name, ROUND(AVG(EXTRACT(YEAR FROM age(current_date, s.birthday)))) AS avg_age
        FROM {schem_name}.rooms r
        INNER JOIN {schem_name}.students s ON r.rooms_id = s.room_id
        GROUP BY r.name
        ORDER BY avg_age ASC
        LIMIT 5;
    """,
    "Select_high_between_ages": f"""
        SELECT r.name,
               MAX(ROUND(EXTRACT(YEAR FROM age(current_date, s.birthday))))
               - MIN(ROUND(EXTRACT(YEAR FROM age(current_date, s.birthday)))) AS age_range
        FROM {schem_name}.rooms r
        INNER JOIN {schem_name}.students s ON r.rooms_id = s.room_id
        GROUP BY r.name
        ORDER BY age_range DESC
        LIMIT 5;
    """,
    "Select_genders": f"""
        SELECT r.rooms_id, r.name
        FROM {schem_name}.rooms r
        JOIN {schem_name}.students s ON s.room_id = r.rooms_id
        GROUP BY r.rooms_id, r.name
        HAVING COUNT(DISTINCT s.sex) > 1;
    """
}