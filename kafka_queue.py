import json
import os

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "career-events")


def get_producer() -> KafkaProducer:
    # creates the kafka client that can send messages
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        # tenantId as key = same tenant stays on same partition
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",  # wait until Kafka saved the message
        retries=3,
    )


def enqueue(producer: KafkaProducer, event: dict) -> None:
    """Put event on the queue. Key = tenantId for isolation."""
    future = producer.send(
        TOPIC,
        key=event["tenantId"],
        value=event,
    )
    future.get(timeout=10)  # raise error if Kafka did not accept it
