import sys
from pathlib import Path

# if __name__ == '__main__' and (__package__ is None or __package__ == ''):
#     file = Path(__file__).resolve()
#     parent, top = file.parent, file.parents[3]

#     sys.path.append(str(top))
#     try:
#         sys.path.remove(str(parent))
#     except ValueError: # Already removed
#         pass

#     import rmq_custom_pack.rpc_producer
#     __package__ = 'rmq_custom_pack.rpc_producer'

from rmq_custom_pack.rpc_producer import Producer
from dagshub import streaming


class FakeDagsHubFilesystem():
    def __init__(self, project_root: str, repo_url: str, token: str):
        self.project_root = project_root
        self.repo_url = repo_url
        self.token = token


    def __new__(self):
        return {'test_key_FDHF': 'test_value_FDHF'}


class TestProducer(Producer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.__host = 'test_host'
        # self.__port = 7000
        # self.__user = 'test_user'
        # self.__password = 'test_passwsord'
        # self.__exchange = 'test_exchange'
        # self.__exchange_type = 'test_exchange_type'
        # self.__queue_request = 'test_queue_request'
        # self.__queue_response = 'test_queue_response'
        # self.__r_key_request = 'test_r_key_request'
        # self.__r_key_response = 'test_r_key_response'


    def test_dugshub_conn(self, project_root: str, repo_url: str, token: str):
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

a = TestProducer(host='test_host',
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





