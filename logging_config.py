# logging_config.py
import logging
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # Ensure logs directory exists

LOG_FILE = os.path.join(LOG_DIR, "etl_pipeline.log")

logging.basicConfig(
    level=logging.INFO,  # Can change to DEBUG for more details
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),   # Logs to file
        logging.StreamHandler()          # Logs to console
    ]
)

logger = logging.getLogger("ETL-Pipeline")
