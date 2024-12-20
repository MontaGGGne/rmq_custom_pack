import sys
import os
import logging
import json
import time
import math
from datetime import datetime
from datetime import timedelta
from . import connections as conn


logging.basicConfig(level=logging.INFO, filename="py_log_consumer.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
logging.basicConfig(level=logging.DEBUG, filename="py_log_consumer_debug.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

class Consumer():
    def __init__(self,
                 key_id: str,
                 secret_key: str,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 exchange: str,
                 exchange_type: str,
                 queue_request: str,
                 queue_response: str,
                 r_key_request: str,
                 r_key_response: str):
        
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__exchange = exchange
        self.__exchange_type = exchange_type
        self.__queue_request = queue_request
        self.__queue_response = queue_response
        self.__r_key_request = r_key_request
        self.__r_key_response = r_key_response

        logging.info(f"[Consumer] Before boto3 connection")
        self.__s3_connection = conn._boto3_connection(key_id, secret_key)
        logging.info("[Consumer] After boto3 connection")

        # print(f"[Consumer] Before pika connection - host: {host}, port: {port}, user: {user}, password: {password}")
        logging.info(f"[Consumer] Before pika connection - host: {host}, port: {port}, user: {user}, password: {password}")
        self.__picka_connection = conn._pika_connection(self.__host,
                                                        self.__port,
                                                        self.__user,
                                                        self.__password)
        # print("[Consumer] After connection")
        logging.info("[Consumer] After picka connection")

        self.__channel = self.__picka_connection.channel()  

        self.__channel.exchange_declare(exchange=self.__exchange,
                                        exchange_type=self.__exchange_type,
                                        durable=True)
        self.__channel.queue_declare(queue=self.__queue_request,
                                     durable=True)
        self.__channel.queue_bind(exchange=self.__exchange,
                                  queue=self.__queue_request,
                                  routing_key=self.__r_key_request)
        self.__channel.queue_declare(queue=self.__queue_response,
                                     durable=True)
        self.__channel.queue_bind(exchange=self.__exchange,
                                  queue=self.__queue_response,
                                  routing_key=self.__r_key_response)


    def consumer_handler(self, bucket: str, time_interval: float, str_datetime_for_use: str):
        begin_datetime = time.time()
        s3_begin_datetime_folder_str = str_datetime_for_use
        s3_begin_datetime_folder_time = datetime.strptime(s3_begin_datetime_folder_str,
                                                          '%Y-%m-%d %H:%M:%S').timestamp()
        s3_time_interval = timedelta(days=1).total_seconds()
        # time_interval = timeout_sec

        def __callback(ch, method, properties, body):
            ch.basic_ack(delivery_tag=method.delivery_tag)

            logging.info(f"Consumer callback body: {body}")
            print(f"Consumer callback body: {body}")

            body_dict = {}
            body_dict = json.loads(body)
            unit_number = body_dict["unit number"]
            time_in_cycles = body_dict["time in cycles"]

            current_datetime = time.time()

            base_dir = "units"
            current_unit = f"unit_number_{unit_number}"
            current_filename = f"time_in_cycles_{time_in_cycles}.json"

            num_interval = math.floor((current_datetime - begin_datetime) / time_interval) # округлить вниз
            s3_datetime_folder_time = s3_begin_datetime_folder_time + s3_time_interval * num_interval
            s3_datetime_folder_loc = time.localtime(s3_datetime_folder_time)
            s3_datetime_folder_str = time.strftime('%Y-%m-%d %H:%M:%S', s3_datetime_folder_loc)

            full_path = os.path.join(base_dir,
                                     s3_datetime_folder_str,
                                     current_unit,
                                     current_filename)
            self.__s3_connection.put_object(Bucket=bucket,
                                            Key=full_path,
                                            Body=json.dumps(body_dict))

            ch.basic_publish(exchange=self.__exchange,
                             routing_key=self.__r_key_response,
                             body=full_path)

        try:
            self.__channel.basic_qos(prefetch_count=1)
            basic_consume_res = self.__channel.basic_consume(queue=self.__queue_request,
                                                             on_message_callback=__callback)
            try:
                # print('[Consumer] Waiting for messages...')
                logging.info('[Consumer] Waiting for messages...')
                self.__channel.start_consuming()
            except Exception as e:
                # print("[Consumer] Start consuming failed!")
                logging.error("[Consumer] Start consuming failed!")
                logging.exception(e)
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            return {'basic_consume_res': basic_consume_res}
        except KeyboardInterrupt:
            # print("[Consumer] Interrupted...")
            logging.info("[Consumer] Interrupted...")
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)