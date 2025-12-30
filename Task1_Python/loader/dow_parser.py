
import wget
import logging
import os
from config.path_config import PathConfig


class Loader:
    """Класс создания парсера с выбором файла"""
    def __init__(self, pc: PathConfig):
        self.pc = pc
        os.makedirs(self.pc.data_dir, exist_ok=True)

    @staticmethod
    def get_url() -> str:
        try:
            logging.info('Введите ссылку на файл:')
            url = input().strip()
            if not url:
                raise ValueError("URL не может быть пустым")
            return url

        except KeyboardInterrupt:
            logging.warning("Ввод прерван пользователем")
            raise
        except EOFError:
            logging.error("Ввод завершён неожиданно (EOF)")
            raise
        except ValueError as e:
            logging.error("Ошибка ввода: %s", e)
            raise

    def download_file(self, url, file_name):
        try:
            path = os.path.join(self.pc.data_dir, file_name)
            wget.download(url, path)
            logging.info(f'Файл {file_name} скачан и сохранён в {path}')
            return path
        except Exception as e:
            logging.warning(f'Ошибка при скачивании {file_name}: {e}')
            return None

    def import_files(self):
        logging.info("Выберите файл для скачки")
        logging.info("1: Студенты (students)")
        logging.info("2: Аудитории (rooms)")
        index = int(input("Ваш выбор: "))

        match index:
            case 1:
                url = self.get_url()
                return self.download_file(url, self.pc.file_name_stud)

            case 2:
                url = self.get_url()
                return self.download_file(url, self.pc.file_name_rooms)

            case _:
                logging.warning("Неверный выбор")
                return None
