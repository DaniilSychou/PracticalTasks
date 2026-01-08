from config.path_config import PathConfig
import os
from reports import xml_report, json_report


class Report:
    """Класс экспорта данных в файл"""
    def __init__(self, pc: PathConfig):
        self.pc = pc
        self.pc.output_dir.mkdir(parents=True, exist_ok=True)

        self.exporters = {
            "json": json_report.JsonReporter(),
            "xml": xml_report.XmlReporter()
        }

    def export(self, data, report_name, fmt="json"):
        if fmt not in self.exporters:
            raise ValueError("Формат должен быть 'json' или 'xml'")

        path = self.pc.output_dir / f"{report_name}.{fmt}"
        exporter = self.exporters[fmt]
        exporter.export(data, path)

