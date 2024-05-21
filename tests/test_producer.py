import fake_classes_methods
import os
from typing import Any, Dict


#################################################### Test Methods ####################################################

fake_producer_obj = fake_classes_methods.rpc_producer.Producer(host='test_host',
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

DUGSHUB_CONN_RES: fake_classes_methods.streaming.DagsHubFilesystem = fake_producer_obj._dugshub_conn(repo_url=T_URL,
                                                                                   token=T_TOKEN)


# def test_dugshub_conn():
#     dugshub_conn_result: fake_classes_methods.streaming.DagsHubFilesystem = fake_producer_obj._dugshub_conn(repo_url=T_URL,
#                                                                                    token=T_TOKEN)

#     assert dugshub_conn_result.project_root == '.'
#     assert dugshub_conn_result.repo_url == T_URL
#     assert dugshub_conn_result.token == T_TOKEN


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