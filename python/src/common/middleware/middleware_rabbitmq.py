import pika
import random
import string

import pika.exceptions
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):

        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            
            self.channel.basic_qos(prefetch_count=1)
        except (pika.exceptions.AMQPConnectionError, OSError) as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        
        self.queue_name = queue_name

        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e
        
        self._consumer_tag = None
        self._consuming = False

    def start_consuming(self, on_message_callback):
        
        def wrapper(channel, method, properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        try:
            self._consumer_tag = self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=wrapper, auto_ack=False
            )
            self._consuming = True
            self.channel.start_consuming()
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
            pika.exceptions.ChannelClosed,
            pika.exceptions.ConnectionClosed,
        ) as e:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            self._consuming = False
            raise MessageMiddlewareMessageError(str(e)) from e
        finally:
            self._consuming = False

    def stop_consuming(self):

        try:
            self.channel.stop_consuming()
        except(
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
            pika.exceptions.ConnectionClosed,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e)) from e

    def send(self, message):

        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message
                )
        except(
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
            pika.exceptions.ConnectionClosed,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e
        
    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e)) from e



class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
            
    def __init__(self, host, exchange_name, routing_keys):
        
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            
            self.channel.basic_qos(prefetch_count=1)
        except (pika.exceptions.AMQPConnectionError, OSError) as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        try:
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=False)

            result = self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
            self.queue_name = result.method.queue

            for routing_key in routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e
        
        self._consumer_tag = None
        self._consuming = False
        
    def start_consuming(self, on_message_callback):
        
        def wrapper(channel, method, properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        try:
            self._consumer_tag = self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=wrapper, auto_ack=False
            )
            self._consuming = True
            self.channel.start_consuming()
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
            pika.exceptions.ChannelClosed,
            pika.exceptions.ConnectionClosed,
        ) as e:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            self._consuming = False
            raise MessageMiddlewareMessageError(str(e)) from e
        finally:
            self._consuming = False

    def stop_consuming(self):

        try:
            self.channel.stop_consuming()
        except(
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
            pika.exceptions.ConnectionClosed,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e)) from e
    
    def send(self, message):

        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name, routing_key=routing_key, body=message)
        except(
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
            pika.exceptions.ConnectionClosed,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e
        

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e)) from e