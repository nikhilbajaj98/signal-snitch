import json
import os
import random
import time

import pika

_EXCHANGE = os.getenv("AMQP_EXCHANGE", "telemetry.events")
_ROUTING_KEY = os.getenv("AMQP_ROUTING_KEY", "pulse")
_AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")


def main() -> None:
    params = pika.URLParameters(_AMQP_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=_EXCHANGE, exchange_type="topic", durable=True)

    print(
        f"Starting SignalSnitch AMQP Producer... "
        f"exchange={_EXCHANGE} routing_key={_ROUTING_KEY}"
    )
    try:
        while True:
            data = {
                "timestamp": int(time.time()),
                "status": random.choice(["SUCCESS", "FAILED"]),
                "message": random.choice(
                    ["Hello from AMQP", "This is a test message", "Another AMQP message"]
                ),
            }
            channel.basic_publish(
                exchange=_EXCHANGE,
                routing_key=_ROUTING_KEY,
                body=json.dumps(data).encode("utf-8"),
                properties=pika.BasicProperties(content_type="application/json"),
            )
            print(f"Published to exchange={_EXCHANGE} routing_key={_ROUTING_KEY}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping SignalSnitch AMQP Producer...")
    finally:
        connection.close()
