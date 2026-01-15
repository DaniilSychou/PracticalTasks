import argparse
import logging
import wget
from pathlib import Path

# Импорт модулей
from loader import dow_parser, stud_parser, room_parser
from db_model import db_works
from reports import report
from config.db_config import DBConfig
from config.path_config import PathConfig
from config.queries import QUERIES


def setup_logging():
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("logs/cli_log.log", encoding="utf-8", mode="w"),
            logging.StreamHandler()
        ]
    )

def download_files(path_config: PathConfig):
    loader = dow_parser.Loader(path_config)
    logging.info("Скачиваем файл студентов")
    loader.download_file(path_config.stud_path, path_config.file_name_stud)
    logging.info("Скачиваем файл аудиторий")
    loader.download_file(path_config.room_path, path_config.file_name_rooms)

def parse_files(path_config: PathConfig):
    students = stud_parser.StudentsLoader(path_config.data_dir).load(path_config.file_name_stud)
    rooms = room_parser.RoomsLoader(path_config.data_dir).load(path_config.file_name_rooms)
    return students, rooms

def load_to_db(db_config: DBConfig, path_config: PathConfig, students, rooms):
    db_conn = db_works.PostgresConnection(
        db_config.host, db_config.port, db_config.user, db_config.password, db_config.name
    )
    database = db_works.Database(db_conn, db_config)
    database.create_schem()
    database.create_rooms_table()
    database.create_stud_table()
    database.insert_rooms_table(rooms)
    database.insert_stud_table(students)
    return database

def generate_reports(path_config: PathConfig, database, db_config):
    sel1 = database.select_queries(QUERIES["Select_count_rooms"].format(schema=db_config.schema))
    rep = report.Report(path_config)
    rep.export(sel1, "Report1", "xml")
    rep.export(sel1, "Report2", "json")

def main():
    parser = argparse.ArgumentParser(description="CLI для проекта")
    parser.add_argument("--download", action="store_true", help="Скачать файлы студентов и аудиторий")
    parser.add_argument("--load-db", action="store_true", help="Загрузить данные в базу")
    parser.add_argument("--report", action="store_true", help="Сгенерировать отчёты")
    parser.add_argument("--all", action="store_true", help="Выполнить все действия")
    args = parser.parse_args()

    try:
        setup_logging()
        path_config = PathConfig.from_env()
        db_config = DBConfig.from_env()
        logging.info("CLI запущен")

        if args.download or args.all:
            download_files(path_config)

        if args.load_db or args.all:
            students, rooms = parse_files(path_config)
            database = load_to_db(db_config, path_config, students, rooms)
        else:
            database = None

        if args.report or args.all:
            if database is None:
                students, rooms = parse_files(path_config)
                database = load_to_db(db_config, path_config, students, rooms)
            generate_reports(path_config, database, db_config)

        logging.info("CLI завершил работу успешно")

    except Exception as e:
        logging.exception(f"Ошибка при выполнении CLI: {e}")

if __name__ == "__main__":
    main()
