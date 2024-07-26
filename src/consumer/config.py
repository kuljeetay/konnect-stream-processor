import os

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_HOST", "localhost:9092"),
    "group.id": "cdc-consumer-group",
    "auto.offset.reset": "earliest",  # Start consuming from the beginning
    "enable.auto.commit": False,
}

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cdc-events")

OPENSEARCH_CONFIG = {
    "host": os.getenv("OPENSEARCH_HOST", "localhost"),
    "port": os.getenv("OPENSEARCH_PORT", 9200),
    "use_ssl": False,
    "verify_certs": False,
}

LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s %(levelname)s %(message)s",
    "handlers": [
        {"class": "logging.StreamHandler", "stream": "ext://sys.stdout"},
        {
            "class": "logging.FileHandler",
            "filename": "consumer.log",
            "mode": "a",
        },
    ],
}
