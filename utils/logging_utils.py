import logging
import os
from datetime import datetime

def get_logger(layer_name):
    os.makedirs("data/logs", exist_ok=True)
    log_filename = f"data/logs/{layer_name}_etl_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger(layer_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_filename)
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger