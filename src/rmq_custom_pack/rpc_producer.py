import sys, os, json, logging
from . import connection as conn
from dagshub import streaming


logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
logging.basicConfig(level=logging.DEBUG, filename="py_log_debug.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

class Producer():
    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 exchange,
                 exchange_type,
                 queue_request,
                 queue_response,
                 r_key_request,
                 r_key_response):
        
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
        
        self.__connection = conn._pika_connection(self.__host, self.__port, self.__user, self.__password)
        
        self.__channel = self.__connection.channel()  
        
        self.__channel.exchange_declare(exchange=self.__exchange, exchange_type=self.__exchange_type, durable=True)

        self.__channel.queue_declare(queue=self.__queue_request, durable=True)
        self.__channel.queue_bind(exchange=self.__exchange, queue=self.__queue_request, routing_key=self.__r_key_request)

        self.__channel.queue_declare(queue=self.__queue_response, durable=True)
        self.__channel.queue_bind(exchange=self.__exchange, queue=self.__queue_response, routing_key=self.__r_key_response)


    def __dugshub_conn(self, repo_url, token):
        try:
            return streaming.DagsHubFilesystem(".", repo_url=repo_url, token=token)
        except Exception as e:
            logging.error("[Producer] dugshub_conn: (DagsHubFilesystem) streaming failed!")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def __get_files_from_dugshub(self, url, fs):
        try:
            return fs.http_get(url)
        except Exception as e:
            logging.error("[Producer] get_files_from_dugshub: (http_get) Getting files from storage failed!")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def __data_publish(self, prod_num, csv_file_str):
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
        
                self.__channel.basic_publish(exchange=self.__exchange,
                                            routing_key=self.__r_key_request,
                                            body=json.dumps({data_id: data_line_list_of_dicts}))
                
            pde_logs = self.__connection.process_data_events(time_limit=None)
            logging.info(f"[Producer] data_publish: process_data_events (successful publish) - {pde_logs}")
        except Exception as e:
            logging.error("[Producer] data_publish: publish data failed!")
            logging.exception(e)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0) 


    def producer_handler(self, prod_num, repo_url, token, url_path_storage, filename: str = 'test_FD001.csv'):

        def __callback(ch, method, properties, body):
            print(body)
            logging.debug(f"[Producer] callback: body - {body}")

        try:
            self.__channel.basic_consume(queue=self.__queue_response, on_message_callback=__callback)
            file_system = self.__dugshub_conn(repo_url=repo_url, token=token)
            csv_file_str = self.__get_files_from_dugshub(os.path.join(url_path_storage, filename), file_system)
            self.__data_publish(self, prod_num, csv_file_str)
        except KeyboardInterrupt:
            logging.info("[Producer] Interrupted...")
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)