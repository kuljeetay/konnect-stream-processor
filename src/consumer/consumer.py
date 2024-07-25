from confluent_kafka import Consumer, KafkaError


class MyKafkaConsumer:
    def __init__(self, topic, group_id):
        self.topic = topic
        self.group_id = group_id
        self.config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',  # Start consuming from the beginning
        }
        self.consumer = Consumer(self.config)
    
    def consume_messages(self):
        self.consumer.subscribe([self.topic])
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
            else:
                print(f"Received message: {msg.value().decode('utf-8')}")

if __name__ == "__main__":
    my_topic = "cdc-events"
    my_group_id = "cdc-consumer-group"

    consumer_instance = MyKafkaConsumer(topic=my_topic, group_id=my_group_id)
    consumer_instance.consume_messages()