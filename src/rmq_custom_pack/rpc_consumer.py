import sys
import os
import logging
import json
import time
from datetime import datetime
from pathlib import Path
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
        self.__picka_connection = conn._pika_connection(self.__host, self.__port, self.__user, self.__password)
        # print("[Consumer] After connection")
        logging.info("[Consumer] After picka connection")

        self.__channel = self.__picka_connection.channel()  

        self.__channel.exchange_declare(exchange=self.__exchange, exchange_type=self.__exchange_type, durable=True)

        self.__channel.queue_declare(queue=self.__queue_request, durable=True)
        self.__channel.queue_bind(exchange=self.__exchange, queue=self.__queue_request, routing_key=self.__r_key_request)

        self.__channel.queue_declare(queue=self.__queue_response, durable=True)
        self.__channel.queue_bind(exchange=self.__exchange, queue=self.__queue_response, routing_key=self.__r_key_response)


    def consumer_handler(self, bucket, timeout_sec):

        def __callback(ch, method, properties, body):
            ch.basic_ack(delivery_tag=method.delivery_tag)

            logging.info(f"Consumer callback body: {body}")
            print(f"Consumer callback body: {body}")

            body_dict = {}
            body_dict = json.loads(body)
            unit_number = body_dict["unit number"]
            time_in_cycles = body_dict["time in cycles"]

            current_time = time.time()
            local_time = time.localtime(current_time)
            current_time_str = time.strftime('%Y-%m-%d %H:%M:%S', local_time)
            # cur_day = local_time.tm_mday
            # cur_mounth = local_time.tm_mon
            # cur_year = local_time.tm_year
            # cur_hours = local_time.tm_hour
            # cur_minuts = local_time.tm_min

            base_dir = "units"
            current_time_dir = f"{current_time_str}"
            current_unit = f"unit_number_{unit_number}"
            current_filename = f"time_in_cycles_{time_in_cycles}.json"

            base_dir_prefix = self.__s3_connection.list_objects_v2(Bucket='nasa-turbofans',
                                                                    Prefix=f"{base_dir}/",
                                                                    Delimiter = "/",
                                                                    MaxKeys=1000)
            full_path: str = ''
            if 'CommonPrefixes' not in base_dir_prefix:
                full_path = os.path.join(base_dir, current_time_dir, current_unit, current_filename)
                self.__s3_connection.put_object(Bucket=bucket, Key=full_path, Body=json.dumps(body_dict))
            else:
                last_prefix = base_dir_prefix['CommonPrefixes'][-1]['Prefix'].rstrip('/')
                last_time_dir = last_prefix.split('/')[-1]
                last_time = datetime.strptime(last_time_dir, '%Y-%m-%d %H:%M:%S').timestamp()
                # dir_name_as_list = last_time_dir.split('_')
                # dir_date = dir_name_as_list[0].split('-')
                # dir_time = dir_name_as_list[1].split('-')
                # if cur_day==int(dir_date[0]) and cur_mounth==int(dir_date[1]) and cur_year==int(dir_date[2]) and cur_hours==int(dir_time[0]) and abs(cur_minuts-int(dir_time[1]))<10:
                if abs(last_time - current_time) <= timeout_sec:
                    full_path = os.path.join(last_prefix, current_unit, current_filename)
                    self.__s3_connection.put_object(Bucket=bucket, Key=full_path, Body=json.dumps(body_dict))
                else:
                    full_path = os.path.join(base_dir, current_time_dir, current_unit, current_filename)
                    self.__s3_connection.put_object(Bucket=bucket, Key=full_path, Body=json.dumps(body_dict))

            # all_time_prefix = self.__s3_connection.list_objects_v2(Bucket='nasa-turbofans',
            #                                                         Prefix=f"{current_dir}/",
            #                                                         Delimiter = "/",
            #                                                         MaxKeys=1000)
            # str_for_body: str = ''
            # if 'CommonPrefixes' not in all_time_prefix:
            #     self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(current_time_dir, current_filename), Body=json.dumps(body_dict))
            #     str_for_body = os.path.join(current_time_dir, current_filename)
            # else:
            #     last_prefix = all_time_prefix['CommonPrefixes'][-1]['Prefix'].rstrip('/')
            #     time_folder = last_prefix.split('/')[-1]
            #     dir_name_as_list = time_folder.split('_')
            #     dir_date = dir_name_as_list[0].split('-')
            #     dir_time = dir_name_as_list[1].split('-')
            #     if cur_day==int(dir_date[0]) and cur_mounth==int(dir_date[1]) and cur_year==int(dir_date[2]) and cur_hours==int(dir_time[0]) and abs(cur_minuts-int(dir_time[1]))<10:
            #         self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(last_prefix, current_filename), Body=json.dumps(body_dict))
            #         str_for_body = os.path.join(last_prefix, current_filename)
            #     else:
            #         self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(current_time_dir, current_filename), Body=json.dumps(body_dict))
            #         str_for_body = os.path.join(current_time_dir, current_filename)
                # for prefix in all_time_prefix['CommonPrefixes']:
                #     time_folder = prefix.split('/')[1]
                #     dir_name_as_list = time_folder.split('_')
                #     dir_date = dir_name_as_list[0].split('-')
                #     dir_time = dir_name_as_list[1].split('-')
                #     if cur_day==int(dir_date[0]) and cur_mounth==int(dir_date[1]) and cur_year==int(dir_date[2]) and cur_hours==int(dir_time[0]) and abs(cur_minuts-int(dir_time[1]))<5:
                #         self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(time_folder, current_filename), Body=json.dumps(body_dict))
                #     else:
                #         self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(current_time_dir, current_filename), Body=json.dumps(body_dict))

            ch.basic_publish(exchange=self.__exchange,
                             routing_key=self.__r_key_response,
                             body=full_path)

        try:
            self.__channel.basic_qos(prefetch_count=1)
            basic_consume_res = self.__channel.basic_consume(queue=self.__queue_request, on_message_callback=__callback)
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