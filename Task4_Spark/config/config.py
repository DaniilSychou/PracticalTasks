from pathlib import Path
from dotenv import load_dotenv
import os
import logging


# Load environment variables from a .env file
load_dotenv()

# Database connection constants
const_url = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# Database connection properties
const_props = {"user": os.getenv('DB_USER'), 
               "password": os.getenv('DB_PASSWORD'), 
               "driver": "org.postgresql.Driver"}

BASE_DIR = Path(__file__).resolve().parent.parent

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# Create separate loggers
error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(LOG_DIR / "py_error_log.log", encoding="utf-8", mode="w")
error_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
error_logger.addHandler(error_handler)

results_logger = logging.getLogger("results_logger")
results_logger.setLevel(logging.INFO)
results_handler = logging.FileHandler(LOG_DIR / "log_file.log", encoding="utf-8", mode="w")
results_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
results_logger.addHandler(results_handler)

