import json
import logging
from reports.base_report import BaseReport


class JsonReporter(BaseReport):
    """Реализация интерфейса в json формате"""
    def export(self, data, path):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            logging.info(f"JSON отчёт создан: {path}")
        except Exception as e:
            logging.warning(f"Ошибка при создании JSON отчёта: {e}")
