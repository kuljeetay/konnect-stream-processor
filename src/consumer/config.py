KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': "cdc-consumer-group",
    'auto.offset.reset': 'earliest',  # Start consuming from the beginning
    'enable.auto.commit': False
}

OPENSEARCH_CONFIG = {
    'host': 'localhost',
    'port': 9200,
    'use_ssl': False,
    'verify_certs': False
}

LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s %(levelname)s %(message)s',
    'handlers': [
        {'class': 'logging.StreamHandler', 'stream': 'ext://sys.stdout'},
        {'class': 'logging.FileHandler', 'filename': 'consumer.log', 'mode': 'a'}
    ]
}