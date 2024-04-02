import sys, os, logging
from . import connection as conn

# HOST = 'localhost'
# PORT = 7801
# USER = 'rmuser'
# PASSWORD = 'rmpassword'

# REPO_URL = 'https://dagshub.com/Dimitriy200/Data'
# TOKEN = 'a1482d904ec14cd6e61aa6fcc9df96278dc7c911'
# URL_PATH_STORAGE = 'https://dagshub.com/api/v1/repos/Dimitriy200/Data/raw/82fd8214a8769595e670f10ce0c135947bb6638e'


class Consumer():
    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 exchange,
                 exchange_type,
                 queue_request,
                 queue_response,
                 r_key_request,
                 r_key_response):
        
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
        
        self.__connection = conn._pika_connection(self.__host, self.__port, self.__user, self.__password)
        
        self.__channel = self.__connection.channel()  
        
        self.__channel.exchange_declare(exchange=self.__exchange, exchange_type=self.__exchange_type, durable=True)

        self.__channel.queue_declare(queue=self.__queue_request, durable=True)
        self.__channel.queue_bind(exchange=self.__exchange, queue=self.__queue_request, routing_key=self.__r_key_request)

        self.__channel.queue_declare(queue=self.__queue_response, durable=True)
        self.__channel.queue_bind(exchange=self.__exchange, queue=self.__queue_response, routing_key=self.__r_key_response)


    def consumer_handler(self):

        def __callback(ch, method, properties, body):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(body)
            ch.basic_publish(exchange=self.__exchange, 
                            routing_key=self.__r_key_response, 
                            body=body)

        try:
            self.__channel.basic_qos(prefetch_count=1)
            self.__channel.basic_consume(queue=self.__queue_request, on_message_callback=__callback)
            
            try:
                logging.info('[Consumer] Waiting for messages...')
                self.__channel.start_consuming()
            except Exception as e:
                logging.error("[Consumer] Start consuming failed!")
                logging.exception(e)
                
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
        except KeyboardInterrupt:
            logging.info("[Consumer] Interrupted...")

            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)



# def consumer_handler(host,
#                      port,
#                      user,
#                      password,
#                      exchange,
#                      exchange_type,
#                      queue_request,
#                      queue_response,
#                      r_key_request,
#                      r_key_response):
#     connection = conn._pika_connection(host, port, user, password)
    
#     channel = connection.channel()

#     channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)

#     channel.queue_declare(queue=queue_request, durable=True)
#     channel.queue_bind(exchange=exchange, queue=queue_request, routing_key=r_key_request)

#     channel.queue_declare(queue=queue_response, durable=True)
#     channel.queue_bind(exchange=exchange, queue=queue_response, routing_key=r_key_response)

#     def callback(ch, method, properties, body):
#         ch.basic_ack(delivery_tag=method.delivery_tag)
        
#         print(body)

#         ch.basic_publish(exchange=exchange, 
#                         routing_key=r_key_response, 
#                         body=body)

#     channel.basic_qos(prefetch_count=1)
#     channel.basic_consume(queue=queue_request, on_message_callback=callback)

#     print('[Server_RabbitMQ] Waiting for messages')
#     channel.start_consuming()


# if __name__ == '__main__':
#     try:
#         consumer_handler()
#     except KeyboardInterrupt:
#         print("[Server_RabbitMQ] Interrupted")
#         try:
#             sys.exit(0)
#         except SystemExit:
#             os._exit(0)