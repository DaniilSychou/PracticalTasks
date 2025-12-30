from pathlib import Path
from dataclasses import dataclass
import os
from dotenv import load_dotenv

# вычисляем BASE_DIR один раз
BASE_DIR = Path(__file__).resolve().parent.parent

# загружаем .env
load_dotenv(BASE_DIR / ".env")


@dataclass(frozen=True)
class PathConfig:
    base_dir: Path
    env_path: Path
    file_name_rooms: str
    file_name_stud: str
    data_dir: Path
    output_dir: Path
    stud_path: Path
    room_path: Path

    @classmethod
    def from_env(cls) -> "PathConfig":
        try:
            return cls(
                base_dir=BASE_DIR,
                env_path=BASE_DIR / ".env",
                file_name_rooms=os.environ["FILE_NAME_ROOMS"],
                file_name_stud=os.environ["FILE_NAME_STUD"],
                data_dir=(BASE_DIR / os.environ["DATA_DIR"]).resolve(),
                output_dir=(BASE_DIR / os.environ["OUTPUT_DIR"]).resolve(),
                stud_path=(BASE_DIR / os.environ["STUD_PATH"]).resolve(),
                room_path=(BASE_DIR / os.environ["ROOM_PATH"]).resolve(),
            )
        except KeyError as e:
            raise RuntimeError(f"Missing env variable: {e.args[0]}")
