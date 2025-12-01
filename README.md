# Kafka

This repository provides example producer and consumer scripts using Apache Kafka for simple order processing based on files containing XML order data.

## Contents

- [`producer.py`](https://github.com/nitinsriv/Kafka/blob/main/producer.py): Watches an `input` directory for new XML files describing orders, extracts their order number and name, and sends these as JSON messages to a Kafka topic called `orders`.

- [`consumer.py`](https://github.com/nitinsriv/Kafka/blob/main/consumer.py): Consumes messages from the `orders` Kafka topic, deserializes the order data, and prints the received messages.

## Usage

### Requirements

- Python 3.x
- [`kafka-python`](https://pypi.org/project/kafka-python/) library
- Running Kafka broker on `localhost:9092`
- (Producer only) Order XML files placed in the `input` directory

### Producer

The producer script (`producer.py`) monitors the local `input` directory for XML files expected in the following format:

```xml
<order>
    <number>12345</number>
    <name>BWG1</name>
</order>
```

It reads each XML file, extracts the order number and name, and sends these as JSON messages to the Kafka topic `orders`. The topic and broker address can be changed in the script.

To run the producer:

```bash
python producer.py
```

### Consumer

The consumer script (`consumer.py`) subscribes to the `orders` Kafka topic and prints each received message.

To run the consumer:

```bash
python consumer.py
```

## File Summary

- **producer.py**  
  - Monitors the `input` directory for XML files.
  - Parses order number and name from each XML.
  - Publishes order info to Kafka topic `orders` as JSON.

- **consumer.py**  
  - Subscribes to Kafka topic `orders`.
  - Prints each received JSON order message.

