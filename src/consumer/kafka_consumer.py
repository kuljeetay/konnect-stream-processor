import json
import logging
from confluent_kafka import Consumer, KafkaError
from opensearch_client import OpenSearchClient


class KonnectStreamConsumer:
    def __init__(self, consumer_config, topic):
        self.topic = topic
        self.config = consumer_config
        self.consumer = Consumer(self.config)
        self.consumer.subscribe([self.topic])
        self.opensearch_client = OpenSearchClient()
    
    def consume_messages(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Poll for new messages
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition for topic {msg.topic()} [{msg.partition()}]")
                    else:
                        print(f"Error while consuming: {msg.error()}")
                        # raise KafkaException(msg.error())
                else:
                    print(f"Received message: {msg.value().decode('utf-8')}")
                    # check below
                    self.handle_message(msg)
                    self.consumer.commit(msg)
        except Exception as e:
            logging.error(f"Exception in consume loop: {str(e)}")
        finally:
            self.consumer.close()
    
    def handle_message(self, msg):
        try:
            record = json.loads(msg.value().decode('utf-8'))
            # Extract the 'after' part of the record
            if 'after' in record and record['after'] is not None:
                data = record['after']['value']
                key = record['after']['key']
                response = self.opensearch_client.index_record(
                    index='cdc',                # index name is cdc
                    record=data,
                    record_id=key
                )
                logging.info(f"Message ingested into OpenSearch with ID: {response['_id']}")
        except Exception as e:
            logging.error(f"Failed to process message: {str(e)}")
            raise e