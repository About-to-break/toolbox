import logging
import time
from typing import Callable, Union

import pika


class RabbitBase:
    def __init__(self, logger: logging.Logger, uri: str):
        self.l = logger
        self.uri = uri
        self.parameters = pika.URLParameters(self.uri)
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.channel.Channel | None = None

    def connect(self):
        raise NotImplementedError

    def _ensure_connected(self):
        if (
            self.connection is None
            or self.connection.is_closed
            or self.channel is None
            or self.channel.is_closed
        ):
            self.connect()


# =========================
# PRODUCER
# =========================

class RabbitProducer(RabbitBase):
    def __init__(self, logger: logging.Logger, uri: str, exchange: str, key: str):
        super().__init__(logger, uri)
        self.exchange = exchange
        self.routing_key = key

    def connect(self, delay_seconds: int = 3):
        while True:
            try:
                self.l.info("[producer] connecting to RabbitMQ")
                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()
                self.l.info("[producer] connected")
                return
            except Exception as e:
                self.l.error(f"[producer] connect error: {e}")
                time.sleep(delay_seconds)

    def publish(self, body: Union[bytes, str]) -> bool:
        try:
            self._ensure_connected()
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=body,
            )
            return True
        except pika.exceptions.AMQPError as e:
            self.l.error(f"[producer] publish failed: {e}")
            try:
                if self.connection and not self.connection.is_closed:
                    self.connection.close()
            except Exception:
                pass
            return False


# =========================
# CONSUMER
# =========================

class RabbitConsumer(RabbitBase):
    def __init__(self, logger: logging.Logger, uri: str, queue: str):
        super().__init__(logger, uri)
        self.queue = queue

    def connect(self, delay_seconds: int = 3):
        while True:
            try:
                self.l.info("[consumer] connecting to RabbitMQ")
                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=1)
                self.l.info("[consumer] connected")
                return
            except Exception as e:
                self.l.error(f"[consumer] connect error: {e}")
                time.sleep(delay_seconds)

    def consume(
        self,
        handler_func,
        extra_func,
    ):
        self._ensure_connected()

        def _callback(ch, method, properties, body: bytes):
            self.l.debug(f"[consumer] received message")
            try:
                result = handler_func(body)
                if extra_func is not None:
                    extra_func(result)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                self.l.error(f"[consumer] handler error: {e}")
                # сообщение вернётся в очередь
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        while True:
            try:
                self.channel.basic_consume(
                    queue=self.queue,
                    on_message_callback=_callback,
                    auto_ack=False,
                )
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                self.l.error(f"[consumer] connection lost: {e}")
                self.connect()
            except Exception as e:
                self.l.error(f"[consumer] fatal error: {e}")
                raise
