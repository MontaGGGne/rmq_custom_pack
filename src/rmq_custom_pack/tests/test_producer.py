from .fake_classes_methods import *
import os
from typing import Any, Dict


#################################################### Test Methods ####################################################

PROD_NUM = 1
CSV_FILES_DIR ='test_file_dir'
TIME_SLEEP = 0.1

T_CSV_RES = 'first_col,second_col,third_col\n1,2,3\n1,2,3\n1,2,3'
T_FALENAME = ''


def test_producer_handler(monkeypatch):
    monkeypatch.setattr(rpc_producer.Producer, '_get_csv_info_from_dir', get_csv_info_from_dir_test)
    monkeypatch.setattr(rpc_producer.Producer, '_data_publish', data_publish_test)
    
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
    
    producer_handler_result: Dict[str, Any] = fake_producer_obj.producer_handler(prod_num=PROD_NUM,
                                                                                 csv_files_dir=CSV_FILES_DIR,
                                                                                 filename=T_FALENAME,
                                                                                 time_sleep=TIME_SLEEP)
    print(producer_handler_result)
    
    assert producer_handler_result['basic_consume_res']['test_queue'] == 'test_queue_response'
    assert producer_handler_result['basic_consume_res']['test_auto_ack'] is False
    assert producer_handler_result['basic_consume_res']['test_exclusive'] is False
    assert producer_handler_result['basic_consume_res']['test_consumer_tag'] is None
    assert producer_handler_result['basic_consume_res']['test_arguments'] is None
    
    assert producer_handler_result['list_csv']['dict_csv'] == {'csv_files_dir': 'test_file_dir', 'filename': ''}
    phr_data_publish_res_dict = json.loads(producer_handler_result['data_publish_res'])
    assert phr_data_publish_res_dict['prod_num'] == PROD_NUM
    assert phr_data_publish_res_dict['time_sleep'] == TIME_SLEEP
    assert phr_data_publish_res_dict['filename'] == T_FALENAME