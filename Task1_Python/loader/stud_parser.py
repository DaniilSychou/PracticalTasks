from loader.json_parser import BaseJsonLoader


class StudentsLoader(BaseJsonLoader):
    """Реализацияя чтения файла students.json"""
    def load(self, file_name):
        data = self.read_json(file_name)
        return [
            {
                "students_id": int(r["id"]),
                "name": r["name"],
                "birthday": r["birthday"],
                "sex": r["sex"],
                "room_id": int(r["room"])
            }
            for r in data
        ]
