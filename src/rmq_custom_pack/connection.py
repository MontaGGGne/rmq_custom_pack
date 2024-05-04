import traceback
import pika
import logging
import sys
import os


logging.basicConfig(level=logging.INFO, filename="py_log_connection.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

__all__ = []

def _pika_connection(host: str, port: int, user: str, password: str):
    try:
        print(f"[Connection] Before pika connection - host: {host}, port: {port}, user: {user}, password: {password}")
        logging.info(f"[Connection] Before pika connection - host: {host}, port: {port}, user: {user}, password: {password}")
        # connection = pika.BlockingConnection(
        #             pika.ConnectionParameters(
        #                 host=host,
        #                 port=port,
        #                 credentials=pika.PlainCredentials(
        #                     user,
        #                     password)))
        parameters = pika.URLParameters(f'amqp://{user}:{password}@{host}:{port}/%2F')
        connection = pika.BlockingConnection(parameters)
        
        print("[Connection] After connection")
        logging.info("[Connection] After connection")
    except Exception as e:
        print("[Connection] Pika connection failed")
        print(traceback.format_exc())
        logging.error(traceback.format_exc())
        logging.exception(e)
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    return connection