import sys, os, json
import json
import os
import connection as conn
from dagshub import streaming

# HOST = 'localhost'
# PORT = 7801
# USER = 'rmuser'
# PASSWORD = 'rmpassword'

# REPO_URL = 'https://dagshub.com/Dimitriy200/Data'
# TOKEN = 'a1482d904ec14cd6e61aa6fcc9df96278dc7c911'
# URL_PATH_STORAGE = 'https://dagshub.com/api/v1/repos/Dimitriy200/Data/raw/82fd8214a8769595e670f10ce0c135947bb6638e'


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
    
    
    def __callback(ch, method, properties, body):
        print(body)

    def producer_handler(self, repo_url, token, url_path_storage, filename: str = 'test_FD001.csv'):
        try:
            
            self.__channel.basic_consume(queue=self.__queue_response, on_message_callback=self.__callback)

            fs = streaming.DagsHubFilesystem(".", repo_url=repo_url, token=token)
            csv_file_str = fs.http_get(os.path.join(url_path_storage, filename))
            
            csv_as_list_of_dicts = []
            columns_names = []  
            list_csv = []
            list_csv = csv_file_str.text.split('\n')
            columns_names = list_csv[0].split(',')
            list_csv.pop(0)
            for data_line in list_csv:
                if data_line is None or data_line == '':
                    continue
                dict_from_line_in_csv = {}
                data_list = data_line.split(',')
                for col_id, col_name in enumerate(columns_names):
                    dict_from_line_in_csv[col_name] = data_list[col_id]
                    
                    self.__channel.basic_publish(exchange=self.__exchange,
                                        routing_key=self.__r_key_request,
                                        body=json.dumps(dict_from_line_in_csv))
                    
                csv_as_list_of_dicts.append(dict_from_line_in_csv)
            
            self.__connection.process_data_events(time_limit=None)
            
        except KeyboardInterrupt:
            print("Request Interrupted")
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)



# def producer_handler(host,
#                      port,
#                      user,
#                      password,
#                      exchange,
#                      exchange_type,
#                      queue_request,
#                      queue_response,
#                      r_key_request,
#                      r_key_response):
#     connection = conn._pika_connection(host, port, user, password)
    
#     channel = connection.channel()  
    
#     channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)

#     channel.queue_declare(queue=queue_request, durable=True)
#     channel.queue_bind(exchange=exchange, queue=queue_request, routing_key=r_key_request)

#     channel.queue_declare(queue=queue_response, durable=True)
#     channel.queue_bind(exchange=exchange, queue=queue_response, routing_key=r_key_response)
    
    
#     def callback(ch, method, properties, body):
#         print(body)

#     channel.basic_consume(queue=queue_response, on_message_callback=callback)

#     fs = streaming.DagsHubFilesystem(".", repo_url=REPO_URL, token=TOKEN)
#     csv_file_str = fs.http_get(os.path.join(URL_PATH_STORAGE, 'test_FD001.csv'))
    
#     csv_as_list_of_dicts = []
#     columns_names = []  
#     list_csv = []
#     list_csv = csv_file_str.text.split('\n')
#     columns_names = list_csv[0].split(',')
#     list_csv.pop(0)
#     for data_line in list_csv:
#         if data_line is None or data_line == '':
#             continue
#         dict_from_line_in_csv = {}
#         data_list = data_line.split(',')
#         for col_id, col_name in enumerate(columns_names):
#             dict_from_line_in_csv[col_name] = data_list[col_id]
            
#             channel.basic_publish(exchange=exchange,
#                                   routing_key=r_key_request,
#                                   body=json.dumps(dict_from_line_in_csv))
            
#         csv_as_list_of_dicts.append(dict_from_line_in_csv)
    
#     connection.process_data_events(time_limit=None)

# if __name__ == '__main__':
#     try:
#         producer_handler()
#     except KeyboardInterrupt:
#         print("Request Interrupted")
#         try:
#             sys.exit(0)
#         except SystemExit:
#             os._exit(0)