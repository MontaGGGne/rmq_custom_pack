from pika import adapters
from rmq_custom_pack import rpc_producer
from dagshub import streaming
from rmq_custom_pack import connection as conn


class FakeDagsHubFilesystem():
    def __init__(self, project_root: str, repo_url: str, token: str):
        self.project_root = project_root
        self.repo_url = repo_url
        self.token = token


    def fake_http_get(self, url):
        return {'test_url': url}
    

class FakeBlockingConnection():
    def __init__(self, fake_host: str, fake_port: int, fake_user: str, fake_password: str):
        self.host = fake_host
        self.port = fake_port
        self.user = fake_user
        self.password = fake_password


    def channel(self):
        return {'channel_test_key': 'channel_test_value'}


# Monkey patch
streaming.DagsHubFilesystem = FakeDagsHubFilesystem
adapters.BlockingConnection = FakeBlockingConnection

def fake_pika_connection(host, port, user, password) -> adapters.BlockingConnection:
    connection = adapters.BlockingConnection(fake_host=host,
                                             fake_port=port,
                                             fake_user=user,
                                             fake_password=password)
    return connection

conn._pika_connection = fake_pika_connection

def test_dugshub_conn(self, project_root: str, repo_url: str, token: str) -> streaming.DagsHubFilesystem:
    return streaming.DagsHubFilesystem(project_root=project_root,
                                       repo_url=repo_url,
                                       token=token)


def test_get_files_from_dugshub(self, url: str, fs: streaming.DagsHubFilesystem):
    return fs.fake_http_get(url)


# Monkey patch
# rpc_producer.Producer._connection = 'test_connection'
# rpc_producer.Producer._channel = 'test_channel'
rpc_producer.Producer._dugshub_conn = test_dugshub_conn
rpc_producer.Producer._get_files_from_dugshub = test_get_files_from_dugshub

fake_producer_obj = rpc_producer.Producer(host='test_host',
                                          port=0000,
                                          user='test',
                                          password='test',
                                          exchange='test_exchange',
                                          exchange_type='test_exchange_type',
                                          queue_request='test_queue_request',
                                          queue_response='test_queue_response',
                                          r_key_request='test_r_key_request',
                                          r_key_response='test_r_key_response')

test_project_root = '.'
test_url = 'https://example.html'
test_token = 'TeStToKeN'
dugshub_conn_result: streaming.DagsHubFilesystem = fake_producer_obj._dugshub_conn(project_root = test_project_root,
                                                                                   repo_url = test_url,
                                                                                   token = test_token)
get_files_from_dugshub_result = fake_producer_obj._get_files_from_dugshub(url=test_url, fs=dugshub_conn_result)

# assert picka_conn_result['test_host'] == 'test_host'
# assert picka_conn_result['test_port'] == 0000
# assert picka_conn_result['test_user'] == 'test'
# assert picka_conn_result['test_password'] == 'test'

assert dugshub_conn_result.project_root == test_project_root
assert dugshub_conn_result.repo_url == test_url
assert dugshub_conn_result.token == test_token

assert get_files_from_dugshub_result['test_url'] == test_url





