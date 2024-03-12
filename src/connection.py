import pika


__all__ = []

def _pika_connection(host, port, user, password):
    connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,
                    port=port,
                    credentials=pika.PlainCredentials(
                        user,
                        password)))
    return connection