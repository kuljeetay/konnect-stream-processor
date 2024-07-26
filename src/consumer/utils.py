import logging
import sys
from config import LOGGING_CONFIG


def configure_logging():
    """
    Configures logging settings for the service.
    """
    try:
        logging.basicConfig(
            level=LOGGING_CONFIG["level"],
            format=LOGGING_CONFIG["format"],
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler("consumer.log", mode="a"),
            ],
        )
    except Exception as e:
        logging.error(f"An unexpected error occurred while configuring logging: {e}")

