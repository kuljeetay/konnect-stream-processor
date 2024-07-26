import logging
from kafka_consumer import KonnectStreamConsumer
from utils import configure_logging  # signal handlers needed?
from config import KAFKA_CONFIG, KAFKA_TOPIC


def main():
    configure_logging()
    logging.info("Starting Konnect Stream Consumer...")

    consumer = KonnectStreamConsumer(KAFKA_CONFIG)
    logging.info(
        "Konnect Stream Consumer initialized : "
        + str(consumer.is_initalized(topic=KAFKA_TOPIC))
    )

    consumer.subscribe(topic=KAFKA_TOPIC)

    # setup_signal_handlers(consumer)

    consumer.consume_messages()


if __name__ == "__main__":
    main()
