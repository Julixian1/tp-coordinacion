import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)


class Message:
    
    def __init__(self, channel, delivery_tag, body):
        self._channel = channel
        self._delivery_tag = delivery_tag
        self.body = body

    def ack(self):
        try:
            self._channel.basic_ack(delivery_tag=self._delivery_tag)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión al enviar ACK: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno al enviar ACK: {e}")

    def nack(self, requeue=True):
        try:
            self._channel.basic_nack(delivery_tag=self._delivery_tag, requeue=requeue)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión al enviar NACK: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno al enviar NACK: {e}")


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self._on_message_callback = None

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue = self.queue_name, durable = True)
            self.channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error inicializando cola: {e}")

    def _on_pika_message(self, ch, method, properties, body):
        msg = Message(ch, method.delivery_tag, body)
        if self._on_message_callback:
            self._on_message_callback(msg.body, msg.ack, msg.nack)

    def start_consuming(self, on_message_callback):
        self._on_message_callback = on_message_callback

        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._on_pika_message, auto_ack=False,)
            self.channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(f"Conexión perdida consumiendo: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error en el canal o consumo: {e}")
        
    def stop_consuming(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión al detener: {e}")

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión enviando: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error cerrando conexión: {e}")

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys if isinstance(routing_keys,list) else [routing_keys]
        self.connection = None
        self.channel = None
        self._on_message_callback = None

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()

            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type="topic",
                durable=True,
            )
            result = self.channel.queue_declare(queue="", exclusive=True)
            self.queue_name = result.method.queue

            for rkey in self.routing_keys:
                self.channel.queue_bind(
                    queue=self.queue_name,
                    exchange=self.exchange_name,
                    routing_key=rkey,
                )

            self.channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error inicializando exchange: {e}")

    def _on_pika_message(self, ch, method, properties, body):
        msg = Message(ch, method.delivery_tag, body)
        if self._on_message_callback:
            self._on_message_callback(msg.body, msg.ack, msg.nack)

    def start_consuming(self, on_message_callback):
        self._on_message_callback = on_message_callback
        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self._on_pika_message,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Conexión perdida consumiendo: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error en el canal o consumo: {e}")

    def stop_consuming(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión al detener: {e}")

    def stop_consuming_threadsafe(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.add_callback_threadsafe(self.stop_consuming)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión al detener thread-safe: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno al detener thread-safe: {e}")

    def send(self, message):
        try:
            for rk in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=rk,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent
                    ),
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión enviando: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")

    def send_with_key(self, routing_key, message):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión enviando: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error cerrando conexión: {e}")