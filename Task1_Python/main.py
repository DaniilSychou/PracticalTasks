import logging
from db_model import db_works
from reports import report
from loader import dow_parser, stud_parser, room_parser
from config.db_config import DBConfig as db
from config.path_config import PathConfig as pc
from config.queries import QUERIES


def main():
    """Главная функция программы"""
    #parser = cli.build_parser()
    #args = parser.parse_args()
    #args.func(args)
    try:
        #Создание объектов Config
        db.from_env()
        pc.from_env()

        logging.basicConfig(level=logging.INFO, filename="logs/py_log.log",
                          filemode="w", format="%(asctime)s %(levelname)s %(message)s")
        # Импорт файлов
        logging.info("Программа запущена")
        loader = dow_parser.Loader()
        # 1. скачиваем файлы
        logging.info("Скачиваем файл студентов")

        loader.download_file(
            url = pc.stud_path,
            file_name = pc.file_name_stud
        )
        logging.info("Скачиваем файл аудиторий")
        loader.download_file(
            url=pc.room_path,
            file_name=pc.file_name_rooms
        )
        # 2. парсим JSON
        students = stud_parser.StudentsLoader(pc.data_dir).load(pc.file_name_stud)
        rooms = room_parser.RoomsLoader(pc.data_dir).load(pc.file_name_rooms)

        # загрузка соединения
        db_conn = db_works.PostgresConnection(db.host, db.port, db.user, db.password, db.name)

        # Создание БД
        data_base = db_works.Database(db_conn)
        data_base.create_schem()
        data_base.create_rooms_table()
        data_base.create_stud_table()

        data_base.insert_rooms_table(rooms)
        data_base.insert_stud_table(students)

        sel1 = data_base.select_queries(QUERIES["Select_count_rooms"])

        # создаем репорты
        rep = report.Report(pc.output_dir)
        rep.export(sel1, "Report1", "xml")
        rep.export(sel1, "Report2", "json")

        logging.info("Программа прекратила работу")
    except Exception as e:
        logging.warning(f'Ошибка при выполнении работы программы: {e}')

if __name__ == "__main__":
    main()
