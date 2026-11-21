import argparse
import json
import os

from .lab_connection import open_connection

QUEUE = "order_tasks"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("name", choices=("worker-A", "worker-B"))
    args = parser.parse_args()
    name = args.name
    pid = os.getpid()

    with open_connection() as connection:
        channel = connection.channel()
        channel.queue_declare(
            queue=QUEUE, durable=True, arguments={"x-queue-type": "classic"}
        )
        channel.basic_qos(prefetch_count=1)

        def handle_task(delivery_channel, method, _properties, body):
            task = json.loads(body)
            task_id = task["task_id"]
            seconds = task["seconds"]
            print(
                f"START worker={name} pid={pid} queue={QUEUE} task={task_id} "
                f"seconds={seconds} redelivered={method.redelivered}",
                flush=True,
            )
            connection.sleep(seconds)
            print(f"DONE worker={name} task={task_id}", flush=True)
            delivery_channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"ACK_SENT worker={name} task={task_id}", flush=True)

        channel.basic_consume(
            queue=QUEUE, on_message_callback=handle_task, auto_ack=False
        )
        print(f"WAIT worker={name} pid={pid} queue={QUEUE}", flush=True)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            print(f"STOP worker={name}", flush=True)


if __name__ == "__main__":
    main()
