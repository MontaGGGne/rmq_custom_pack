from rmq_custom_pack import rpc_producer
from dagshub import streaming


class FakeDagsHubFilesystem():
    def __new__(self, project_root: str, repo_url: str, token: str):
        self.project_root = project_root
        self.repo_url = repo_url
        self.token = token

        return {'test_key_FDHF': 'test_value_FDHF'}


class TestProducer():
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


    def test_dugshub_conn(self, project_root: str, repo_url: str, token: str) -> streaming.DagsHubFilesystem:
        # project_root = '.'
        # repo_url = 'https://example.html'
        # token = 'dfdggg'

        streaming.DagsHubFilesystem = FakeDagsHubFilesystem

        return streaming.DagsHubFilesystem(project_root=project_root, repo_url=repo_url, token=token)

        # test = Producer()._dugshub_conn(project_root, repo_url, token)

        # assert(isinstance(test, FakeDagsHubFilesystem))
        # assert(project_root == test.project_root)
        # assert(repo_url == test.repo_url)
        # assert(token == test.token)

rpc_producer.Producer = TestProducer
a = rpc_producer.Producer(host='test_host',
                 port=7000,
                 user = 'test_user',
                 password = 'test_passwsord',
                 exchange = 'test_exchange',
                 exchange_type = 'test_exchange_type',
                 queue_request = 'test_queue_request',
                 queue_response = 'test_queue_response',
                 r_key_request = 'test_r_key_request',
                 r_key_response = 'test_r_key_response')
b = a.test_dugshub_conn(project_root = '.', repo_url = 'https://example.html', token = 'dfdggg')
print(b)

# class MockResponse:
#     # метод всегда возвращает определенный словарь для тестов
#     @staticmethod
#     def json():
#         return {"mock_key": "mock_response"}

# def test_get_json(monkeypatch):
#     # этой функции могут быть переданы любые аргументы, но она всегда 
#     # будет возвращать объект `MockResponse` с методом `.json()`.
#     def mock_get(*args, **kwargs):
#         return MockResponse()

#     # Изменим поведение `requests.get()` поведением функции `mock_get()`
#     monkeypatch.setattr(requests, "get", mock_get)

#     # тестируем ответ `app.get_json` из модуля `app.py`
#     result = app.get_json("https://fakeurl")
#     assert result["mock_key"] == "mock_response"





