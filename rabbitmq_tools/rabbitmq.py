from typing import Union

import pika
import time
import logging
import threading
from functools import partial


class RabbitBase:
    def __init__(self, logger: logging.Logger, uri: str):
        self.l = logger
        self.uri = uri
        self.parameters = pika.URLParameters(self.uri)
        self.connection = None
        self.channel = None

    def connect(self):
        pass

    def _ensure_connected(self):
        if (
                self.connection is None
                or self.connection.is_closed
                or self.channel is None
                or self.channel.is_closed
        ):
            self.connect()


class RabbitProducer(RabbitBase):
    def __init__(self, logger: logging.Logger, uri: str, exchange: str, key: str):
        super().__init__(logger, uri)

        self.routing_key = key
        self.exchange = exchange

        self._lock = threading.Lock()

    def connect(self, delay_seconds: int = 3):
        while True:
            try:
                self.l.info("Connecting to RabbitMQ...")

                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()

                self.l.info(
                    f"Successfully connected producer to exchange '{self.exchange}' "
                    f"using key '{self.routing_key}'"
                )
                return

            except Exception as e:
                self.l.error(f"Error connecting to RabbitMQ: {e}")
                self.l.info(f"Retrying in {delay_seconds} seconds...")
                time.sleep(delay_seconds)

    def publish(self, body: Union[bytes, str]):
        with self._lock:
            try:
                self._ensure_connected()
                self.channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                    body=body,
                )
                self.l.debug("Message published to RabbitMQ")
                return True

            except Exception as e:
                self.l.error(f"Publish failed: {e}")
                self.l.info("Reconnecting and retrying...")

                # Переподключение
                self.connect()

                try:
                    self.channel.basic_publish(
                        exchange=self.exchange,
                        routing_key=self.routing_key,
                        body=body,
                    )
                    self.l.debug("Message re-published after reconnect")
                    return True
                except Exception as e2:
                    self.l.error(f"Retry failed: {e2}")
                    return False


class RabbitConsumer(RabbitBase):
    def __init__(self, logger: logging.Logger, uri: str, queue: str, heartbeat_interval: int = 5):
        super().__init__(logger, uri)
        self.queue = queue
        self._lock = threading.Lock()

        self.heartbeat_interval = heartbeat_interval
        self._stop_heartbeat = threading.Event()

    def consume(self, callback_func):

        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()

        while True:
            with self._lock:
                try:
                    self._ensure_connected()
                    self.channel.basic_consume(
                        queue=self.queue,
                        on_message_callback=partial(self._track_callback, callback_func=callback_func),
                        auto_ack=False,
                    )
                    self.channel.start_consuming()
                except pika.exceptions.AMQPConnectionError as e:
                    self.l.error(f"AMQP connection failed: {e}")
                    self.connect()
                    continue
                except Exception as e2:
                    self.l.error(f"Unexpected error consuming from RabbitMQ: {e2}")
                    raise
                finally:
                    self._stop_heartbeat.set()

    def connect(self, delay_seconds: int = 3):
        while True:
            try:
                self.l.info("Connecting to RabbitMQ...")

                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=1)

                self.l.info(f"Successfully connected consumer")
                return

            except Exception as e:
                self.l.error(f"Error connecting to RabbitMQ: {e}")
                self.l.info(f"Retrying in {delay_seconds} seconds...")
                time.sleep(delay_seconds)

    def _track_callback(self, channel, method, properties, body, callback_func):
        self.l.debug(f"Message received from RabbitMQ queue {self.queue}")
        try:
            callback_func(body)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            self.l.error(f"Error while processing RabbitMQ message: {e}")

    def _heartbeat_loop(self):
        while not self._stop_heartbeat.is_set():
            try:
                if self.connection and self.connection.is_open:
                    pass
            except Exception as e:
                self.l.error(f"RabbitMQ heartbeat thread error: {e}")
            time.sleep(self.heartbeat_interval)
