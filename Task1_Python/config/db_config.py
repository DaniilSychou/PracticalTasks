import logging
import os
from dataclasses import dataclass

@dataclass
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    name: str
    schema: str

    @classmethod
    def from_env(cls) -> 'DBConfig':
        try:
            return cls(
                host=os.environ["DB_HOST"],
                port=int(os.environ.get("DB_PORT", 5432)),
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASS"],
                name=os.environ["DB_NAME"],
                schema=os.environ["SCHEMA"]
            )
        except KeyError as ke:
            logging.error("Произошла ошибка загрузки данных: %s", ke.args[0])
            raise RuntimeError(f"Missing env variable: {ke.args[0]}")
        except ValueError as ve:
            logging.error("Произошла ошибка типа данных DB_PORT: %s", ve.args[0])
            raise RuntimeError("DB_PORT must be integer")







