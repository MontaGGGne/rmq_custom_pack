import sys, os, json, logging
from . import connection as conn
from dagshub import streaming


logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
logging.basicConfig(level=logging.DEBUG, filename="py_log_debug.log",filemode="w",
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
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._queue_request = queue_request
        self._queue_response = queue_response
        self._r_key_request = r_key_request
        self._r_key_response = r_key_response

        self._connection = conn._pika_connection(self._host,
                                                  self._port,
                                                  self._user,
                                                  self._password)
        self._channel = self._connection.channel()


    def producer_handler(self, prod_num, repo_url, token, url_path_storage, filename: str = 'test_FD001.csv'):
        self._channel.exchange_declare(exchange=self._exchange,
                                        exchange_type=self._exchange_type,
                                        durable=True)

        self._channel.queue_declare(queue=self._queue_request,
                                     durable=True)
        self._channel.queue_bind(exchange=self._exchange,
                                  queue=self._queue_request,
                                  routing_key=self._r_key_request)

        self._channel.queue_declare(queue=self._queue_response, durable=True)
        self._channel.queue_bind(exchange=self._exchange,
                                  queue=self._queue_response,
                                  routing_key=self._r_key_response)

        def _callback(ch, method, properties, body):
            print(body)
            logging.debug(f"[Producer] callback: body - {body}")

        try:
            self._channel.basic_consume(queue=self._queue_response, on_message_callback=_callback)
            file_system = self._dugshub_conn(repo_url=repo_url, token=token)
            csv_file_str = self._get_files_from_dugshub(os.path.join(url_path_storage, filename), file_system)
            self._data_publish(self, prod_num, csv_file_str)
        except KeyboardInterrupt:
            logging.info("[Producer] Interrupted...")
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def _dugshub_conn(self, repo_url: str, token: str) -> streaming.DagsHubFilesystem:
        try:
            return streaming.DagsHubFilesystem(".", repo_url=repo_url, token=token)
        except Exception as e:
            logging.error("[Producer] dugshub_conn: (DagsHubFilesystem) streaming failed!")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def _get_files_from_dugshub(self, url: str, fs: streaming.DagsHubFilesystem):
        try:
            return fs.http_get(url)
        except Exception as e:
            logging.error("[Producer] get_files_from_dugshub: (http_get) Getting files from storage failed!")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def _data_publish(self, prod_num, csv_file_str):
        try:
            list_csv = csv_file_str.text.split('\n')
            columns_names = list_csv[0].split(',')
            data_list = []
            list_csv.pop(0)
            for data_id, data_line in enumerate(list_csv):
                if data_line is None or data_line == '':
                    continue
                data_line_list_of_dicts = []
                data_list.append(data_line.split(','))
                data_line_list_of_dicts = [{col_name: float(col_val)} for col_name, col_val in zip(columns_names, data_line.split(','))]
                data_line_list_of_dicts.append({'prod_num': prod_num})
        
                self._channel.basic_publish(exchange=self._exchange,
                                            routing_key=self._r_key_request,
                                            body=json.dumps({data_id: data_line_list_of_dicts}))
                
            pde_logs = self._connection.process_data_events(time_limit=None)
            logging.info(f"[Producer] data_publish: process_data_events (successful publish) - {pde_logs}")
        except Exception as e:
            logging.error("[Producer] data_publish: publish data failed!")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)