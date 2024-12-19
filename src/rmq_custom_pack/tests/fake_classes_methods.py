import json
from pika import adapters
from pika import connection
from pika import credentials
from typing import Any, Dict

from .. import rpc_producer
from .. import rpc_consumer
from .. import connections as conn


#################################################### Fake External Methods ####################################################
#-------------------------------------------------------- For Boto3 --------------------------------------------------------

def boto3_connection_test(key_id: str, secret_key: str) -> Any:
    return {'key_id': key_id, 'secret_key': secret_key}

#-------------------------------------------------------- For Producer --------------------------------------------------------

def get_csv_info_from_dir_test(self, csv_files_dir: str, filename: str):
    return {"dict_csv": {'csv_files_dir': csv_files_dir,
                         'filename': filename},
            "units_list": ['test_unit'],
            "columns_names": ['test_col']}
    
def data_publish_test(self, prod_num, dict_csv, units_list, columns_names, time_sleep: float, filename: str):
    res_dict = {'prod_num': prod_num,
                'time_sleep': time_sleep,
                'filename': filename}
    return json.dumps(res_dict)

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
        self.impl = fake_channel_impl
        self.connection = fake_connection

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
    
    def basic_ack(self,
                  delivery_tag=0,
                  multiple=False):
        return {'test_delivery_tag': delivery_tag,
                'test_multiple': multiple}
    
    def basic_qos(self,
                  prefetch_size: int = 0,
                  prefetch_count: int = 0,
                  global_qos: bool = False):
        return {'test_prefetch_size': prefetch_size,
                'test_prefetch_count': prefetch_count,
                'test_global_qos': global_qos}
    
    def start_consuming(self):
        return {'test_connection': self.connection}


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
