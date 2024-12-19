import traceback
import pika
import logging
import sys
import os
import boto3


logging.basicConfig(level=logging.INFO, filename="py_log_connection.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

__all__ = []

def _pika_connection(host: str, port: int, user: str, password: str):
    try:
        # print(f"[Connection] Before pika connection - host: {host}, port: {port}, user: {user}, password: {password}")
        logging.info(f"[Connection] Before pika connection - host: {host}, port: {port}, user: {user}, password: {password}")
        parameters = pika.URLParameters(f'amqp://{user}:{password}@{host}:{port}/%2F')
        connection = pika.BlockingConnection(parameters)
        # print("[Connection] After connection")
        logging.info("[Connection] After picka connection")
    except Exception as e:
        # print("[Connection] Pika connection failed")
        # print(traceback.format_exc())
        logging.error(traceback.format_exc())
        logging.exception(e)
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    return connection


def _boto3_connection(key_id: str, secret_key: str):
    try:
        logging.info(f"[Connection] Before boto3 connection")
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id = key_id,
            aws_secret_access_key = secret_key)
        logging.info("[Connection] After boto3 connection")
    except Exception as e:
        # print("[Connection] Boto3 connection failed")
        # print(traceback.format_exc())
        logging.error(traceback.format_exc())
        logging.exception(e)
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    return s3
    