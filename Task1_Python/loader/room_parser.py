from loader.json_parser import BaseJsonLoader


class RoomsLoader(BaseJsonLoader):
    """Реализацияя чтения файла rooms.json"""
    def load(self, file_name: str):
        data = self.read_json(file_name)
        return [
            {
                "rooms_id": int(r["id"]),
                "name": r["name"]
            }
            for r in data
        ]
