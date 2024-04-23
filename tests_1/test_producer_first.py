import os
from pika import adapters
from pika import connection
from pika import credentials
from rmq_custom_pack import rpc_producer
from dagshub import streaming
from rmq_custom_pack import connection as conn

from typing import Any, Dict


#################################################### Fake External Methods ####################################################

#-------------------------------------------------------- For DagsHub --------------------------------------------------------
class FakeDagsHubFilesystem():
    def __init__(self, project_root: str, repo_url: str, token: str):
        self.project_root = project_root
        self.repo_url = repo_url
        self.token = token

    def fake_http_get(self, url: str):
        return url
#-------------------------------------------------------- For DagsHub --------------------------------------------------------


#-------------------------------------------------------- For Picka --------------------------------------------------------

######## Classes ########
class FakePlainCredentials():
    def __init__(self, fake_user: str, fake_password: str):
        self.user = fake_user
        self.password = fake_password


# Monkey patch
credentials.PlainCredentials = FakePlainCredentials

class FakeConnectionParameters():
    def __init__(self, fake_host: str, fake_port: int, fake_credentials: credentials.PlainCredentials):
        self.host = fake_host
        self.port = fake_port
        self.credentials = fake_credentials


class FakeBlockingChannel():
    def __init__(self, fake_channel_impl = None, fake_connection = None):
        self._impl = fake_channel_impl
        self._connection = fake_connection

    def exchange_declare(self,
                         exchange: str,
                         exchange_type: str,
                         passive: bool = False,
                         durable: bool = False,
                         auto_delete: bool = False,
                         internal: bool = False) -> Any:
        return {'test_exchange': exchange,
                'test_exchange_type': exchange_type,
                'test_passive': passive,
                'test_durable': durable,
                'test_auto_delete': auto_delete,
                'test_internal': internal}

    def queue_declare(self,
                      queue: Any,
                      passive: bool = False,
                      durable: bool = False,
                      exclusive: bool = False,
                      auto_delete: bool = False,
                      arguments: Any | None = None) -> Any:
        return {'test_queue': queue,
                'test_passive': passive,
                'test_durable': durable,
                'test_exclusive': exclusive,
                'test_auto_delete': auto_delete,
                'test_arguments': arguments}

    def queue_bind(self,
                   queue: Any,
                   exchange: Any,
                   routing_key: Any | None = None,
                   arguments: Any | None = None) -> Any:
        return {'test_queue': queue,
                'test_exchange': exchange,
                'test_routing_key': routing_key,
                'test_arguments':arguments}
    
    def basic_consume(self,
                      queue: Any,
                      on_message_callback: Any,
                      auto_ack: bool = False,
                      exclusive: bool = False,
                      consumer_tag: Any | None = None,
                      arguments: Any | None = None) -> Any:
        return {'test_queue': queue,
                'test_on_message_callback': on_message_callback,
                'test_auto_ack': auto_ack,
                'test_exclusive': exclusive,
                'test_consumer_tag': consumer_tag,
                'test_arguments': arguments}

    def basic_publish(self,
                      exchange: str,
                      routing_key: str,
                      body: str | bytes,
                      properties: None = None,
                      mandatory: bool = False) -> Any:
        return {'test_exchange': exchange,
                'test_routing_key': routing_key,
                'test_body': body,
                'test_properties': properties,
                'test_mandatory': mandatory}
        


# Monkey patch
connection.ConnectionParameters = FakeConnectionParameters

class FakeBlockingConnection():
    def __init__(self, fake_parametrs: connection.ConnectionParameters):
        self.parametrs = fake_parametrs

    def channel(self):
        channel = FakeBlockingChannel(self.parametrs, self)
        return channel
    
    def process_data_events(self, time_limit: int = 0) -> Any:
        return time_limit


######## Functions ########
# Monkey patch
streaming.DagsHubFilesystem = FakeDagsHubFilesystem
adapters.BlockingConnection = FakeBlockingConnection

def fake_pika_connection(host: str, port: int, user: str, password: str) -> adapters.BlockingConnection:
    conn = adapters.BlockingConnection(
        connection.ConnectionParameters(
            fake_host=host,
            fake_port=port,
            fake_credentials=credentials.PlainCredentials(
                user,
                password)))
    return conn

# Monkey patch
conn._pika_connection = fake_pika_connection
#-------------------------------------------------------- For Picka --------------------------------------------------------


#################################################### Fake Producer Methods ####################################################

def fake_dugshub_conn(self, repo_url: str, token: str) -> streaming.DagsHubFilesystem:
    return streaming.DagsHubFilesystem(project_root='.',
                                       repo_url=repo_url,
                                       token=token)


def fake_get_files_from_dugshub(self, url: str, fs: streaming.DagsHubFilesystem):
    return fs.fake_http_get(url)


# Monkey patch
rpc_producer.Producer._dugshub_conn = fake_dugshub_conn
rpc_producer.Producer._get_files_from_dugshub = fake_get_files_from_dugshub


#################################################### Test Methods ####################################################

fake_producer_obj = rpc_producer.Producer(host='test_host',
                                          port=0,
                                          user='test_user',
                                          password='test_pass',
                                          exchange='test_exchange',
                                          exchange_type='test_exchange_type',
                                          queue_request='test_queue_request',
                                          queue_response='test_queue_response',
                                          r_key_request='test_r_key_request',
                                          r_key_response='test_r_key_response')

T_URL = 'https://example.html'
T_TOKEN = 'TeStToKeN'
PROD_NUM = 1

T_CSV_RES = 'first_col,second_col,third_col\n1,2,3\n1,2,3\n1,2,3'
T_FALENAME = ''

DUGSHUB_CONN_RES: streaming.DagsHubFilesystem = fake_producer_obj._dugshub_conn(repo_url=T_URL,
                                                                                   token=T_TOKEN)

get_files_from_dugshub_result = fake_producer_obj._get_files_from_dugshub(url=T_URL, fs=DUGSHUB_CONN_RES)

producer_handler_result: Dict[str, Any] = fake_producer_obj.producer_handler(prod_num=PROD_NUM, repo_url=T_URL , token=T_TOKEN , url_path_storage=T_CSV_RES, filename=T_FALENAME)
data_publish_result = producer_handler_result['data_publish_res']
print(producer_handler_result)


def test_dugshub_conn():
    dugshub_conn_result: streaming.DagsHubFilesystem = fake_producer_obj._dugshub_conn(repo_url=T_URL,
                                                                                   token=T_TOKEN)

    assert dugshub_conn_result.project_root == '.'
    assert dugshub_conn_result.repo_url == T_URL
    assert dugshub_conn_result.token == T_TOKEN


def test_get_files_from_dugshub():
    get_files_from_dugshub_result = fake_producer_obj._get_files_from_dugshub(url=T_URL, fs=DUGSHUB_CONN_RES)

    assert get_files_from_dugshub_result == T_URL


def test_producer_handler():
    producer_handler_result: Dict[str, Any] = fake_producer_obj.producer_handler(prod_num=PROD_NUM,
                                                                                 repo_url=T_URL,
                                                                                 token=T_TOKEN,
                                                                                 url_path_storage=T_CSV_RES,
                                                                                 filename=T_FALENAME)
    
    assert producer_handler_result['basic_consume_res']['test_queue'] == 'test_queue_response'
    assert producer_handler_result['basic_consume_res']['test_auto_ack'] is False
    assert producer_handler_result['basic_consume_res']['test_exclusive'] is False
    assert producer_handler_result['basic_consume_res']['test_consumer_tag'] is None
    assert producer_handler_result['basic_consume_res']['test_arguments'] is None

    assert producer_handler_result['file_system'].project_root == '.'
    assert producer_handler_result['file_system'].repo_url == T_URL
    assert producer_handler_result['file_system'].token == T_TOKEN

    assert producer_handler_result['csv_file_str'] == os.path.join(T_CSV_RES, T_FALENAME)

    list_for_test = [
        [{'first_col': '1'}, {'second_col': '2'}, {'third_col': '3'}, {'prod_num': 1}],
        [{'first_col': '1'}, {'second_col': '2'}, {'third_col': '3'}, {'prod_num': 1}],
        [{'first_col': '1'}, {'second_col': '2'}, {'third_col': '3\\'}, {'prod_num': 1}]
    ]
    assert producer_handler_result['data_publish_res'] == list_for_test





