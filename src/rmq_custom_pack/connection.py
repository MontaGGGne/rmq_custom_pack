import pika, logging, sys, os


__all__ = []

def _pika_connection(host, port, user, password):
    try:
        connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=host,
                        port=port,
                        credentials=pika.PlainCredentials(
                            user,
                            password)))
    except Exception as e:
        logging.error("Pika connection failed")
        logging.exception(e)
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    return connection