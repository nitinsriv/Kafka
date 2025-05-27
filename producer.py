import os
import time
import re
import json
from kafka import KafkaProducer
import xml.etree.ElementTree as ET

# Kafka Order Producer
# This script listens for new files in a specified directory and sends order information to a Kafka topic.

WATCH_DIR = "input"  # Directory to watch for new files
KAFKA_TOPIC = "orders"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

class OrderProducer:
    """
    A Kafka producer that sends order information to a specified topic.
    """
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            batch_size=16384,
            linger_ms=10,
            max_request_size=1048576,  # 1MB max message size
            retries=3
        )

def extract_order_info(file_path):
    """
    Extracts order number and name from an XML file.
    Assumes XML structure like:
    <order>
        <number>12345</number>
        <name>BWG1</name>
    </order>
    """
    order_number = None
    order_name = None
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        number_elem = root.find("number")
        name_elem = root.find("name")
        if number_elem is not None:
            order_number = number_elem.text
        if name_elem is not None:
            order_name = name_elem.text
    except Exception as e:
        print(f"Error parsing XML file {file_path}: {e}")
    return order_number, order_name


def create_kafka_message(order_number, order_name):
    """
    Creates a JSON message for Kafka.
    """
    return json.dumps({
        "order_number": order_number,
        "order_name": order_name
    }).encode("utf-8")


def listen_for_files(directory, producer):
    seen_files = set()
    while True:
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if filename not in seen_files and os.path.isfile(file_path):
                order_number, order_name = extract_order_info(file_path)
                print(f"File: {filename} | Order Number: {order_number} | Name: {order_name}")
                kafka_message = create_kafka_message(order_number, order_name)
                producer.send(KAFKA_TOPIC, kafka_message)
                seen_files.add(filename)
        time.sleep(2)  # Poll every 2 seconds

if __name__ == "__main__":
    if not os.path.exists(WATCH_DIR):
        os.makedirs(WATCH_DIR)
    print(f"Listening for files in '{WATCH_DIR}' directory...")
    oProducer = OrderProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    listen_for_files(WATCH_DIR, oProducer.producer)