import pika


def open_connection():
    credentials = pika.PlainCredentials("demo", "demo-local")
    parameters = pika.ConnectionParameters(
        host="127.0.0.1",
        port=5672,
        virtual_host="/",
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=15,
    )
    return pika.BlockingConnection(parameters)
