from abc import ABC, abstractmethod


class BaseReport(ABC):

    @abstractmethod
    def export(self, data, path):
        """Экспортирует данные в файл"""
        pass
