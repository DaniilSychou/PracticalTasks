from abc import ABC, abstractmethod
import json


class BaseJsonLoader(ABC):
    """Интерфейс создания парсера json файла"""
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def read_json(self, file_name):
        with open(self.data_dir / file_name, encoding="utf-8") as f:
            return json.load(f)

    @abstractmethod
    def load(self, file_name):
        """Каждый наследник обязан реализовать этот метод"""
        pass
