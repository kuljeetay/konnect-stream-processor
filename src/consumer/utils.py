import logging
import sys
import signal
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


def shutdown_hook(consumer):
    def shutdown(signum, frame):
        logging.info("Shutting down consumer...")
        consumer.consumer.close()
        sys.exit(0)

    return shutdown


# def setup_signal_handlers(consumer):
#     signal.signal(signal.SIGINT, shutdown_hook(consumer))
#     signal.signal(signal.SIGTERM, shutdown_hook(consumer))
