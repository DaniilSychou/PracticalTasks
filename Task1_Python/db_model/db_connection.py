import psycopg
import logging


class PostgresConnection:
    """Класс подключения  и обработки базы данных"""
    def __init__(self, host, port, user, password, db):
        self.conn = psycopg.connect(host=host, port=port, user=user, password=password, dbname=db)

    def execute(self, sql, params=None, many=False):
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
        return True

    def executemany(self, sql, rows):
        with self.conn.cursor() as cur:
            cur.executemany(sql, rows)

    def query(self, sql, params=None):
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params or ())
                cols = [d.name for d in cur.description]
                rows = cur.fetchall()  # нужно вызвать здесь, пока курсор открыт
            return [dict(zip(cols, r)) for r in rows]
        except Exception as e:
            logging.warning(f'Ошибка при выполнении запроса {sql}: {e}')
            return None

    def commit(self): self.conn.commit()