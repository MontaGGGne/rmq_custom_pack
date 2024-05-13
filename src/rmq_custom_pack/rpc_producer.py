import sys
import os
import json
import logging
import traceback
from . import connection as conn
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


    def producer_handler(self, prod_num: int, repo_url: str, token: str, url_path_storage: str, filename: str):
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
            file_system = self._dugshub_conn(repo_url=repo_url, token=token)
            csv_file_str = self._get_files_from_dugshub(os.path.join(url_path_storage, filename), file_system)
            data_publish_res = self._data_publish(prod_num, csv_file_str)
            return {'basic_consume_res': basic_consume_res,
                    'file_system': file_system,
                    'csv_file_str': csv_file_str,
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


    def _dugshub_conn(self, repo_url: str, token: str) -> streaming.DagsHubFilesystem:
        try:
            stream = streaming.DagsHubFilesystem(".", repo_url=repo_url, token=token)
            print(f"[Producer] dugshub_conn: (DagsHubFilesystem) {stream}")
            logging.info(f"[Producer] dugshub_conn: (DagsHubFilesystem) {traceback.format_exc()}")
            return stream
        except Exception as e:
            print(f"[Producer] dugshub_conn: (DagsHubFilesystem) {traceback.format_exc()}")
            logging.error(f"[Producer] dugshub_conn: (DagsHubFilesystem) {traceback.format_exc()}")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def _get_files_from_dugshub(self, url: str, fs: streaming.DagsHubFilesystem):
        try:
            return fs.http_get(url)
        except Exception as e:
            print(f"[Producer] get_files_from_dugshub: (http_get) {traceback.format_exc()}")
            logging.error(f"[Producer] get_files_from_dugshub: (http_get) {traceback.format_exc()}")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def _data_publish(self, prod_num, csv_file_str):
        try:
            try:
                list_csv = csv_file_str.text.split('\n')
            except:
                list_csv = csv_file_str.split('\n')
            columns_names = list_csv[0].split(',')
            data_list = []
            full_data_list = []
            list_csv.pop(0)
            for data_id, data_line in enumerate(list_csv):
                if data_line is None or data_line == '':
                    continue
                data_line_list_of_dicts = []
                data_list.append(data_line.split(','))
                data_line_list_of_dicts = [{col_name: col_val} for col_name, col_val in zip(columns_names, data_line.split(','))]
                data_line_list_of_dicts.append({'prod_num': prod_num})
        
                self.__channel.basic_publish(exchange=self.__exchange,
                                            routing_key=self.__r_key_request,
                                            body=json.dumps(data_line_list_of_dicts))

                full_data_list.append(data_line_list_of_dicts)

            pde_logs = self.__connection.process_data_events(time_limit=None)
            print(f"[Producer] data_publish: process_data_events (successful publish) - {pde_logs}")
            logging.info(f"[Producer] data_publish: process_data_events (successful publish) - {pde_logs}")
            return full_data_list
        except Exception as e:
            print(f"[Producer] data_publish: {traceback.format_exc()}")
            logging.error(f"[Producer] data_publish: {traceback.format_exc()}")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)