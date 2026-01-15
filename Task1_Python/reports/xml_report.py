import xml.etree.ElementTree as ET
import logging
from reports.base_report import BaseReport


class XmlReporter(BaseReport):
    """Реализация интерфейса в xml формате"""
    def export(self, data, path):
        try:
            root = ET.Element("Report")
            for row in data:
                row_elem = ET.SubElement(root, "Row")
                for col_name, value in row.items():
                    col_elem = ET.SubElement(row_elem, col_name)
                    col_elem.text = str(value)

            tree = ET.ElementTree(root)
            ET.indent(tree, space="  ", level=0)
            tree.write(path, encoding="utf-8", xml_declaration=True)

            logging.info(f"XML отчёт создан: {path}")
        except Exception as e:
            logging.warning(f"Ошибка при создании XML отчёта: {e}")
