"""
Worker — reads from Kafka and hands off to Event Consumer.

INPUT:  events from Kafka topic `career-events`
OUTPUT: same event POSTed to Event Consumer
        if still failing after retries → DLQ topic
"""

import json
import os
import time
import urllib.error
import urllib.request

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "career-events")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "career-events-dlq")
CONSUMER_URL = os.getenv("CONSUMER_URL", "http://127.0.0.1:3002/consume")
GROUP_ID = os.getenv("KAFKA_GROUP", "career-events-workers")
MAX_RETRIES = 3


def handoff_to_consumer(event: dict) -> None:
    """Send event to Event Consumer. Raise if they say fail."""
    body = json.dumps(event).encode("utf-8")
    req = urllib.request.Request(
        CONSUMER_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            if res.status >= 400:
                raise RuntimeError(f"Consumer failed: {res.status}")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Consumer failed: {e.code}") from e


def send_to_dlq(producer: KafkaProducer, event: dict, error: str) -> None:
    producer.send(
        DLQ_TOPIC,
        key=event.get("tenantId", "unknown").encode("utf-8"),
        value=json.dumps(
            {
                "event": event,
                "error": error,
                "failedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        ).encode("utf-8"),
    )
    producer.flush() 
    # forcefully sending without waiting


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # only commit after success
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )

    dlq_producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

    print(f"Worker listening on topic '{TOPIC}'...")

    for msg in consumer:
        event = msg.value
        event_id = event.get("eventId")
        tenant_id = event.get("tenantId")

        if not tenant_id:
            send_to_dlq(dlq_producer, event, "missing tenantId")
            consumer.commit()
            print(f"DLQ (no tenant): {event_id}")
            continue

        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                handoff_to_consumer(event)
                consumer.commit()
                print(f"Delivered: {event_id} ({event.get('eventType')})")
                last_error = None
                break
            except Exception as e:
                last_error = str(e)
                print(f"Retry {attempt}/{MAX_RETRIES}: {event_id} — {last_error}")
                time.sleep(2 ** attempt)  # 2s, 4s, 8s

        if last_error:
            send_to_dlq(dlq_producer, event, last_error)
            consumer.commit()
            print(f"Moved to DLQ: {event_id}")


if __name__ == "__main__":
    main()
