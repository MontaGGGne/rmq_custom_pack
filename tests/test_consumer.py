import fake_classes_methods


#################################################### Test Methods ####################################################

fake_consumer_obj = fake_classes_methods.rpc_consumer.Consumer(host='test_host',
                                                               port=0,
                                                               user='test_user',
                                                               password='test_pass',
                                                               exchange='test_exchange',
                                                               exchange_type='test_exchange_type',
                                                               queue_request='test_queue_request',
                                                               queue_response='test_queue_response',
                                                               r_key_request='test_r_key_request',
                                                               r_key_response='test_r_key_response')


def test_consumer_handler():
    producer_handler_result = fake_consumer_obj.consumer_handler()

    assert producer_handler_result['basic_consume_res']['test_queue'] == 'test_queue_request'
    assert producer_handler_result['basic_consume_res']['test_auto_ack'] is False
    assert producer_handler_result['basic_consume_res']['test_exclusive'] is False
    assert producer_handler_result['basic_consume_res']['test_consumer_tag'] is None
    assert producer_handler_result['basic_consume_res']['test_arguments'] is None


def test_basic_qos():
    basic_qos_res = fake_consumer_obj._Consumer__channel.basic_qos()

    assert basic_qos_res['test_prefetch_size'] == 0
    assert basic_qos_res['test_prefetch_count'] == 0
    assert basic_qos_res['test_global_qos'] == False


def test_basic_ack():
    basic_ack_res = fake_consumer_obj._Consumer__channel.basic_ack()

    assert basic_ack_res['test_delivery_tag'] == 0
    assert basic_ack_res['test_multiple'] == False


def test_start_consuming():
    start_consuming_res = fake_consumer_obj._Consumer__channel.start_consuming()

    assert start_consuming_res['test_connection'].parametrs.host == 'test_host'
    assert start_consuming_res['test_connection'].parametrs.port == 0



