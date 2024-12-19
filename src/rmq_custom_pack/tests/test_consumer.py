from .fake_classes_methods import *

#################################################### Test Methods ####################################################

fake_consumer_obj = rpc_consumer.Consumer(key_id='test_key_id',
                                          secret_key='test_secret_key',
                                          host='test_host',
                                          port=0,
                                          user='test_user',
                                          password='test_pass',
                                          exchange='test_exchange',
                                          exchange_type='test_exchange_type',
                                          queue_request='test_queue_request',
                                          queue_response='test_queue_response',
                                          r_key_request='test_r_key_request',
                                          r_key_response='test_r_key_response')

def test_consumer_handler(monkeypatch):
    monkeypatch.setattr(conn, '_boto3_connection', boto3_connection_test)
    producer_handler_result = fake_consumer_obj.consumer_handler('test_bucket',
                                                                 1.0,
                                                                 '2000-01-01 00:00:00')

    assert producer_handler_result['basic_consume_res']['test_queue'] == 'test_queue_request'
    assert producer_handler_result['basic_consume_res']['test_auto_ack'] is False
    assert producer_handler_result['basic_consume_res']['test_exclusive'] is False
    assert producer_handler_result['basic_consume_res']['test_consumer_tag'] is None
    assert producer_handler_result['basic_consume_res']['test_arguments'] is None


def test_start_consuming():
    start_consuming_res = fake_consumer_obj._Consumer__channel.start_consuming()

    assert start_consuming_res['test_connection'].parametrs.host == 'test_host'
    assert start_consuming_res['test_connection'].parametrs.port == 0



