import logging
from confluent_kafka import Consumer, KafkaError, KafkaException
from events_handler import EventsHandler


class KonnectStreamConsumer:
    def __init__(self, consumer_config):
        self.topic = None
        self.config = consumer_config
        self.consumer = Consumer(self.config)

    def is_initalized(self, topic):
        """
        Is Initalized method.
        Get the topic from the Kafka Cluster
        :param: topic: Input to get the topic from the kafka cluster.
        :return: topics if exists else False.
        """
        try:
            cluster_metadata = self.consumer.list_topics(topic=topic)
            return cluster_metadata.topics
        except KafkaException:
            return False

    def subscribe(self, topic):
        """
        Subscribe method.
        Subscribe to a particular topic.
        :param: topic: topic to subscribe to.
        :return: None / raise Exception if error
        """
        try:
            self.topic = topic
            self.consumer.subscribe([topic])
        except KafkaException:
            raise

    def consume_messages(self):
        """
        Consume messages method.
        Poll for the events, get the events and process them.
        Polls for a single event, processes and does a commit.
        """
        try:

            while True:
                msg = self.consumer.poll(1.0)  # Poll for new messages
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info(
                            f"Reached end of partition for topic {msg.topic()} [{msg.partition()}]"
                        )
                    else:
                        logging.info(f"Error while consuming: {msg.error()}")
                else:
                    logging.info(
                        f"Received message: {msg.value().decode('utf-8')}"
                    )
                    events_handler = EventsHandler(msg=msg)
                    events_handler.handle_message()
                    self.consumer.commit(msg)
        except Exception as e:
            logging.error(f"Exception in consume loop: {str(e)}")
        finally:
            self.consumer.close()
