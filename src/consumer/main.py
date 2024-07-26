import logging
from kafka_consumer import KonnectStreamConsumer
from utils import configure_logging  # signal handlers needed?
from config import KAFKA_CONFIG, KAFKA_TOPIC


def main():
    try:
        configure_logging()
        logging.info("Starting Konnect Stream Consumer...")

        consumer = KonnectStreamConsumer(KAFKA_CONFIG)
        logging.info(
            "Konnect Stream Consumer initialized : "
            + str(consumer.is_initalized(topic=KAFKA_TOPIC))
        )

        consumer.subscribe(topic=KAFKA_TOPIC)

        consumer.consume_messages()
    except Exception as ex:
        logging.exception(str(ex))


if __name__ == "__main__":
    main()
