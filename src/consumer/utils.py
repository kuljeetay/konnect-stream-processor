import logging
import sys
from config import LOGGING_CONFIG


def configure_logging():
    logging.basicConfig(
        level=LOGGING_CONFIG["level"],
        format=LOGGING_CONFIG["format"],
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("consumer.log", mode="a"),
        ],
    )

