import argparse
import json

import pika

from .lab_connection import open_connection

EXCHANGE = "orders.events"
BINDINGS = {
    "order_audit": "order.*",
    "order_notifications": "order.created",
}
EVENTS = (
    ("EVT-201", "order.created"),
    ("EVT-202", "order.cancelled"),
    ("EVT-203", "payment.failed"),
)


def setup(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    for queue, binding_key in BINDINGS.items():
        channel.queue_declare(
            queue=queue, durable=True, arguments={"x-queue-type": "classic"}
        )
        channel.queue_bind(exchange=EXCHANGE, queue=queue, routing_key=binding_key)
        print(f"BOUND exchange={EXCHANGE} queue={queue} key={binding_key}")


def publish(channel):
    channel.confirm_delivery()
    for event_id, key in EVENTS:
        event = {"event_id": event_id, "order_id": "ORD-1042"}
        try:
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=key,
                body=json.dumps(event).encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                    message_id=event_id,
                ),
                mandatory=True,
            )
        except pika.exceptions.UnroutableError as error:
            reason = error.messages[0].method.reply_text
            print(f"RETURN event={event_id} key={key} reason={reason}")
            if key != "payment.failed":
                raise
        else:
            print(f"CONFIRMED event={event_id} key={key}")
            if key == "payment.failed":
                raise RuntimeError("payment.failed was routed; check the bindings")


def read(channel, queue):
    count = 0
    while True:
        method, _properties, body = channel.basic_get(queue=queue, auto_ack=False)
        if method is None:
            print(f"EMPTY queue={queue} read={count}")
            return
        event = json.loads(body)
        print(
            f"READ queue={queue} event={event['event_id']} "
            f"key={method.routing_key} order={event['order_id']}",
            flush=True,
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        count += 1


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("setup")
    commands.add_parser("publish")
    reader = commands.add_parser("read")
    reader.add_argument("queue", choices=tuple(BINDINGS))
    args = parser.parse_args()

    with open_connection() as connection:
        channel = connection.channel()
        if args.command == "setup":
            setup(channel)
        elif args.command == "publish":
            publish(channel)
        else:
            read(channel, args.queue)


if __name__ == "__main__":
    main()
