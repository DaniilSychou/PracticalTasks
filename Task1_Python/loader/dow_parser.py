from traceback import print_tb

import wget
import logging
from pathlib import Path

from numpy import dtype

from config.path_config import PathConfig


class Loader:
    """Класс загрузки файлов"""
    def __init__(self, pc: PathConfig):
        self.pc = pc
        self.pc.data_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def get_url() -> str:
        try:
            logging.info("Введите ссылку на файл:")
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

    def download_file(self, url: str, file_name: str) -> Path | None:
        try:
            path: Path = self.pc.data_dir / file_name
            wget.download(url, str(path))  # ⬅ здесь преобразуем Path в строку
            logging.info("Файл %s скачан и сохранён в %s", file_name, path)
            return path
        except Exception as e:
            logging.error("Ошибка при скачивании %s: %s", file_name, e)
            return None

    def import_files(self):
        logging.info("Выберите файл для скачки")
        logging.info("1: Студенты (students)")
        logging.info("2: Аудитории (rooms)")

        try:
            index = int(input("Ваш выбор: "))
        except ValueError:
            logging.warning("Введено не число")
            return None

        match index:
            case 1:
                return self.download_file(
                    self.get_url(),
                    self.pc.file_name_stud
                )

            case 2:
                return self.download_file(
                    self.get_url(),
                    self.pc.file_name_rooms
                )

            case _:
                logging.warning("Неверный выбор")
                return None
