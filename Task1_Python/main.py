import logging
import sys
from db_model import db_works
from reports import report
from loader import dow_parser, stud_parser, room_parser
from config.db_config import DBConfig
from config.path_config import PathConfig
from config.queries import QUERIES


def main():
    """Главная функция программы"""
    #parser = cli.build_parser()
    #args = parser.parse_args()
    #args.func(args)
    try:
        #Создание объектов Config
        db_config = DBConfig.from_env()
        path_config = PathConfig.from_env()

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[
                logging.FileHandler("logs/py_log.log", encoding="utf-8", mode="w")
            ]
        )

        # Импорт файлов
        logging.info("Программа запущена")
        loader = dow_parser.Loader(path_config)
        # 1. скачиваем файлы
        logging.info("Скачиваем файл студентов")

        loader.download_file(
            url = path_config.stud_path,
            file_name = path_config.file_name_stud
        )
        logging.info("Скачиваем файл аудиторий")
        loader.download_file(
            url=path_config.room_path,
            file_name=path_config.file_name_rooms
        )
        # 2. парсим JSON
        students = stud_parser.StudentsLoader(path_config.data_dir).load(path_config.file_name_stud)
        rooms = room_parser.RoomsLoader(path_config.data_dir).load(path_config.file_name_rooms)

        # загрузка соединения
        db_conn = db_works.PostgresConnection(db_config.host, db_config.port, db_config.user, db_config.password, db_config.name)

        # Создание БД
        data_base = db_works.Database(db_conn,db_config)
        data_base.create_schem()
        data_base.create_rooms_table()
        data_base.create_stud_table()

        data_base.insert_rooms_table(rooms)
        data_base.insert_stud_table(students)

        sel1 = data_base.select_queries(QUERIES["Select_count_rooms"].format(schema=db_config.schema))

        # создаем репорты
        rep = report.Report(path_config)
        rep.export(sel1, "Report1", "xml")
        rep.export(sel1, "Report2", "json")

        logging.info("Программа прекратила работу")
    except Exception as e:
       logging.warning(f'Ошибка при выполнении работы программы: {e}')

if __name__ == "__main__":
    main()
