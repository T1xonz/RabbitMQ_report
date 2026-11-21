import argparse
import json

import pika

from .lab_connection import open_connection

QUEUE = "order_tasks"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("seconds", type=int)
    parser.add_argument("task_ids", nargs="+")
    args = parser.parse_args()
    if not 1 <= args.seconds <= 120:
        parser.error("seconds must be between 1 and 120")
    if len(set(args.task_ids)) != len(args.task_ids):
        parser.error("task IDs in one batch must be unique")

    with open_connection() as connection:
        channel = connection.channel()
        channel.queue_declare(
            queue=QUEUE, durable=True, arguments={"x-queue-type": "classic"}
        )
        channel.confirm_delivery()

        for task_id in args.task_ids:
            task = {"task_id": task_id, "seconds": args.seconds}
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE,
                body=json.dumps(task).encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                    message_id=task_id,
                ),
                mandatory=True,
            )
            print(
                f"CONFIRMED queue={QUEUE} task={task_id} seconds={args.seconds}",
                flush=True,
            )


if __name__ == "__main__":
    main()
