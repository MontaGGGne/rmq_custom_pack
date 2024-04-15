from ..src.rmq_custom_pack.rpc_producer import Producer


class FakeDagsHubFilesystem():
    def __init__(self, project_root: str, repo_url: str, token: str):
        self.project_root = project_root
        self.repo_url = repo_url
        self.token = token


class TestProducer(Producer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def test_dugshub_conn(self):
        project_root = '.'
        repo_url = 'https://example.html'
        token = 'dfdggg'

        test = Producer()._dugshub_conn(project_root, repo_url, token)

        assert(isinstance(test, FakeDagsHubFilesystem))
        assert(project_root == test.project_root)
        assert(repo_url == test.repo_url)
        assert(token == test.token)



class MockResponse:
    # метод всегда возвращает определенный словарь для тестов
    @staticmethod
    def json():
        return {"mock_key": "mock_response"}

def test_get_json(monkeypatch):
    # этой функции могут быть переданы любые аргументы, но она всегда 
    # будет возвращать объект `MockResponse` с методом `.json()`.
    def mock_get(*args, **kwargs):
        return MockResponse()

    # Изменим поведение `requests.get()` поведением функции `mock_get()`
    monkeypatch.setattr(requests, "get", mock_get)

    # тестируем ответ `app.get_json` из модуля `app.py`
    result = app.get_json("https://fakeurl")
    assert result["mock_key"] == "mock_response"





