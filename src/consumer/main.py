import logging
from kafka_consumer import KonnectStreamConsumer
from consumer.utils import configure_logging, setup_signal_handlers  #signal handlers needed?
from config import KAFKA_CONFIG

def main():
    configure_logging()
    logging.info("Starting Kafka consumer...")

    topic = "cdc-events"
    
    consumer = KonnectStreamConsumer(KAFKA_CONFIG, topic)

    # setup_signal_handlers(consumer)

    consumer.consume_messages()

if __name__ == "__main__":
    main()
