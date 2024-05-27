import sys
import os
import json
import logging
import traceback
from . import connections as conn
from dagshub import streaming


logging.basicConfig(level=logging.INFO, filename="py_log_producer.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
logging.basicConfig(level=logging.DEBUG, filename="py_log_producer_debug.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

class Producer():
    def __init__(self,
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

        print(f"[Producer] Before pika connection - host: {host}, port: {port}, user: {user}, password: {password}")
        logging.info(f"[Producer] Before pika connection - host: {host}, port: {port}, user: {user}, password: {password}")
        self.__connection = conn._pika_connection(self.__host,
                                                  self.__port,
                                                  self.__user,
                                                  self.__password)
        print("[Producer] After connection")
        logging.info("[Producer] After connection")

        self.__channel = self.__connection.channel()


    def producer_handler(self, prod_num: int, csv_files_dir: str, filename: str):
        self.__channel.exchange_declare(exchange=self.__exchange,
                                        exchange_type=self.__exchange_type,
                                        durable=True)

        self.__channel.queue_declare(queue=self.__queue_request,
                                     durable=True)
        self.__channel.queue_bind(exchange=self.__exchange,
                                  queue=self.__queue_request,
                                  routing_key=self.__r_key_request)

        self.__channel.queue_declare(queue=self.__queue_response, durable=True)
        self.__channel.queue_bind(exchange=self.__exchange,
                                  queue=self.__queue_response,
                                  routing_key=self.__r_key_response)

        def _callback(ch, method, properties, body):
            print(body)
            logging.debug(f"[Producer] callback: body - {body}")

        try:
            basic_consume_res = self.__channel.basic_consume(queue=self.__queue_response, on_message_callback=_callback)
            list_csv = self._get_csv_from_dir(csv_files_dir, filename)
            data_publish_res = self._data_publish(prod_num, list_csv, filename)
            return {'basic_consume_res': basic_consume_res,
                    'list_csv': list_csv,
                    'data_publish_res': data_publish_res}
    
        except KeyboardInterrupt:
            print("[Producer] Interrupted...")
            logging.info("[Producer] Interrupted...")
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
        except Exception:
            print(f"[Producer] producer_handler: {traceback.format_exc()}")
            logging.error(f"[Producer] producer_handler: {traceback.format_exc()}")
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def _get_csv_from_dir(self, csv_files_dir: str, filename: str):
        try:
            csv_var: str = ""
            list_csv: list = []
            with open(os.path.join(csv_files_dir, filename), 'r') as csv_f:
                csv_var = csv_f.read()
                for i in csv_var.split('\n'):
                    list_csv.append(i.split(','))
            return list_csv
        except Exception as e:
            print(f"[Producer] get_csv_from_dir: {traceback.format_exc()}")
            logging.error(f"[Producer] get_csv_from_dir: {traceback.format_exc()}")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def _data_publish(self, prod_num, list_csv, filename: str):
        try:
            prod_num = int(prod_num)
        except Exception as e:
            print(f"[Producer] data_publish: {traceback.format_exc()}")
            logging.error(f"[Producer] data_publish: {traceback.format_exc()}")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
        try:
            columns_names = list_csv[0]

            # remove string with columns names
            list_csv.pop(0)

            # remove strings for prod num
            for i in range(prod_num-1):
                list_csv.pop(0)

            # post strings in request queue
            data_list = []
            buffer_data_id: int = 0
            for data_id, data_line in enumerate(list_csv):

                if data_line is None or data_line == '':
                    continue
                if buffer_data_id+3 != data_id:
                    continue
                buffer_data_id = data_id

                obj_with_dicts = {}
                [obj_with_dicts.update({col_name: col_val}) for col_name, col_val in zip(columns_names, data_line)]
                obj_with_dicts.update({'prod num': prod_num})
                obj_with_dicts.update({'time interval': data_id})
                train_type = filename.rstrip('.csv').split('_')[-1]
                obj_with_dicts.update({'train type': train_type})

                print(f"[Producer] data_publish: obj_with_dicts - {obj_with_dicts}")
                logging.info(f"[Producer] data_publish: obj_with_dicts - {obj_with_dicts}")
                data_list.append(obj_with_dicts)
                self.__channel.basic_publish(exchange=self.__exchange,
                                             routing_key=self.__r_key_request,
                                             body=json.dumps(obj_with_dicts))

            # pde_logs = self.__connection.process_data_events(time_limit=None)
            # print(f"[Producer] data_publish: process_data_events (successful publish) - {pde_logs}")
            # logging.info(f"[Producer] data_publish: process_data_events (successful publish) - {pde_logs}")
            print(f"[Producer] data_publish: data_list (successful publish) - {data_list}")
            logging.info(f"[Producer] data_publish: data_list (successful publish) - {data_list}")
            return json.dumps(obj_with_dicts)
        except Exception as e:
            print(f"[Producer] data_publish: {traceback.format_exc()}")
            logging.error(f"[Producer] data_publish: {traceback.format_exc()}")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)