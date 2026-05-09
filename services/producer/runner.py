import json
import random
import time

from confluent_kafka import Producer

_CONF = {"bootstrap.servers": "localhost:9092"}


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def main() -> None:
    producer = Producer(_CONF)
    print("Starting SignalSnitch Producer... Sending pulses to Kafka.")
    try:
        while True:
            data = {
                "timestamp": int(time.time()),
                "status": random.choice(["SUCCESS", "FAILED"]),
                "message": random.choice(
                    ["Hello, world!", "This is a test message", "Another message"]
                ),
            }
            producer.produce(
                "orders",
                json.dumps(data).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping SignalSnitch Producer...")
    finally:
        producer.flush()
