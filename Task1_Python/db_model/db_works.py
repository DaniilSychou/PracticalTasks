from config.db_config import DBConfig
import logging
from db_model.db_connection import PostgresConnection


class Database:
    """Класс для работы с базой данных"""
    def __init__(self, conn: PostgresConnection, db: DBConfig):
        self.conn = conn
        self.db = db

    def create_schem(self):
        try:
            self.conn.execute(f"""CREATE SCHEMA IF NOT EXISTS {self.db.schema};""")
            logging.info(f'Схема {self.db.schema} создана успешно')
            self.conn.commit()
        except Exception as e:
            logging.warning(f'Ошибка при создании {self.db.schema}: {e}')
            return None

    def create_stud_table(self):
        try:
            self.conn.execute(f"""CREATE TABLE IF NOT EXISTS {self.db.schema}.students (
            students_id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            birthday DATE,
            sex CHAR,
            room_id INT,
            CONSTRAINT fk_room
            FOREIGN KEY (room_id)
            REFERENCES {self.db.schema}.rooms(rooms_id)
            ON DELETE SET NULL );
            """)
            self.conn.commit()
            logging.info(f'Таблица Students создана успешно')
        except Exception as e:
            logging.warning(f'Ошибка при создании таблицы Student: {e}')
            return None

    def create_rooms_table(self):
        try:
            self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.db.schema}.rooms (
            rooms_id SERIAL PRIMARY KEY,
            name VARCHAR(50)    );
            """)
            self.conn.commit()
            logging.info(f'Таблица Rooms создана успешно')
        except Exception as e:
            logging.warning(f'Ошибка при создании таблицы Rooms: {e}')
            return None

    def insert_stud_table(self, students):
        try:
            sql = f"""
                INSERT INTO {self.db.schema}.students (students_id, name, sex, birthday, room_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (students_id) DO NOTHING;
            """
            rows = [
                (s["students_id"], s["name"], s["sex"], s["birthday"], s["room_id"])
                for s in students
            ]
            self.conn.executemany(sql, rows)
            self.conn.commit()
            logging.info(f'Добавление данных в таблицу Students прошло успешно')
        except Exception as e:
            logging.warning(f'Ошибка при добавлении данных в таблицу Students: {e}')
            return None

    def insert_rooms_table(self, rooms):
        try:
            sql = f"""
                INSERT INTO {self.db.schema}.rooms (rooms_id, name)
                VALUES (%s, %s)
                ON CONFLICT (rooms_id) DO NOTHING;
            """
            rows = [
                (room["rooms_id"], room["name"])
                for room in rooms
            ]
            self.conn.executemany(sql, rows)
            self.conn.commit()
            logging.info(f'Добавление данных в таблицу Rooms прошло успешно')
        except Exception as e:
            logging.warning(f'Ошибка при добавлении данных в таблицу Rooms: {e}')
            return None

    def select_queries(self, sel_query):
        try:
            return self.conn.query(sel_query)
        except Exception as e:
            logging.warning(f'Ошибка при выполнении {self.db.schema}: {e}')
            return None

    def create_stud_index(self, students):
        try:
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_students_id ON students(students_id);")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_students_room ON students(room_id);")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_students_birth ON students(birthday);")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_students_sex ON students(sex);")
            self.conn.commit()
            logging.info(f'Индексы таблицы Students успешно созданы')
        except Exception as e:
            logging.warning(f'Ошибка при создании индексов в Students: {e}')
            return None

    def create_rooms_index(self):
        try:
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rooms_id ON rooms(rooms_id);")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rooms_name ON rooms(name);")
            self.conn.commit()
            logging.info(f'Индексы таблицы Rooms успешно созданы')
        except Exception as e:
            logging.warning(f'Ошибка при создании индексов в Rooms: {e}')
            return None
