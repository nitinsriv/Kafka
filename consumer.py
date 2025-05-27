from kafka import KafkaConsumer
import json

KAFKA_TOPIC = "orders"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

class OrderConsumer:
    """
    A Kafka consumer that listens for order messages from a specified topic.
    """
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='order-consumer-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

if __name__ == "__main__":
    oConsumer = OrderConsumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    print(f"Subscribed to topic '{KAFKA_TOPIC}'. Waiting for messages...")
    oConsumer.consumer.subscribe([KAFKA_TOPIC])
    for message in oConsumer.consumer:
        print(f"Received message: {message.value}")

