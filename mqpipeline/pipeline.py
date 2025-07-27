"""mq pipline class"""
import logging
import queue
from threading import Thread, Event, Lock
import time

from pika import URLParameters, BasicProperties
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    ChannelWrongStateError,
    ConnectionClosedByBroker
)
from pika.adapters.blocking_connection import BlockingConnection

from .config import MQPipelineConfig

logger = logging.getLogger(__name__)


class MQPipeline:
    """MQPipeline class for managing message queue operations."""

    def __init__(self, config: MQPipelineConfig, single_message_handler: callable):
        """Initialize MQPipeline with the provided configuration."""
        self._config = config
        self._single_message_handler = single_message_handler
        self._internal_queues = {
            "processing_queue": queue.Queue(),
            "ack_queue": queue.Queue()
        }
        self._internal_threads = {
            "stop_event": Event(),
            "consumer_thread": Thread(target=self._process_messages, daemon=True),
            "subscriber_thread": Thread(target=self._run_consumer, daemon=True)
        }
        self._publisher_conninfo_lock = Lock()
        self._publisher_conninfo = {
            "publisher_connection": None,
            "publisher_channel": None,
        }

    def _connect_rabbitmq(self, url:str, queue_name:str):
        """Connect to RabbitMQ using the provided URL and queue name."""
        try:
            logger.debug("Connecting to RabbitMQ at %s for queue %s", url, queue_name)
            parameters = URLParameters(url)
            if parameters.client_properties is None:
                parameters.client_properties = {}

            parameters.client_properties["connection_name"] = f"{self._config.mq_application}-{queue_name}"
            parameters.heartbeat = 60
            parameters.retry_delay = 5
            parameters.connection_attempts = 5
            parameters.socket_timeout = 5
            parameters.blocked_connection_timeout = 30

            connection = BlockingConnection(parameters)

            return connection, connection.channel()
        except Exception as e:
            logger.error("Failed to connect to RabbitMQ: %s", e)
            raise RuntimeError(f"Failed to connect to RabbitMQ: {e}") from e

    def _ensure_publisher_connection(self):
        """Ensure the publisher connection is established."""
        with self._publisher_conninfo_lock:
            publisher_connection = self._publisher_conninfo.get("publisher_connection")
            publisher_channel = self._publisher_conninfo.get("publisher_channel")
            if not publisher_connection or publisher_connection.is_closed:
                url = f"amqp://{self._config.mq_user}:{self._config.mq_password}@{self._config.mq_host}/{self._config.mq_vhost}"
                publisher_connection, publisher_channel = self._connect_rabbitmq(url, self._config.publisher_queue)
            # create queue if it does not exist with durable and quorum settings
            publisher_channel.queue_declare(
                queue=self._config.publisher_queue,
                durable=True,
                arguments={
                    "x-queue-type": "quorum"
                }
            )
            # Ensure the exchange exists
            publisher_channel.exchange_declare(
                exchange=self._config.publisher_exchange,
                exchange_type='direct',
                durable=True
            )
            # Bind the queue to the exchange with the routing key
            publisher_channel.queue_bind(
                queue=self._config.publisher_queue,
                exchange=self._config.publisher_exchange,
                routing_key=self._config.publisher_routing_key
            )

            self._publisher_conninfo["publisher_connection"] = publisher_connection
            self._publisher_conninfo["publisher_channel"] = publisher_channel

    def _publish(self, message):
        """Publish a message to the configured exchange and routing key."""
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                self._ensure_publisher_connection()
                publisher_channel = self._publisher_conninfo["publisher_channel"]
                publisher_channel.basic_publish(
                    exchange=self._config.publisher_exchange,
                    routing_key=self._config.publisher_routing_key,
                    body=message,
                    properties=BasicProperties(
                        delivery_mode=2,  # Make message persistent
                    )
                )
                logger.debug("Message published successfully.")
                return
            except (ConnectionClosedByBroker, AMQPConnectionError, ChannelClosedByBroker, ChannelWrongStateError) as e:
                retries += 1
                logger.error("Failed to publish message: %s. Retrying %d/%d", e, retries, max_retries)
                self._publisher_conninfo["publisher_connection"] = None
                self._publisher_conninfo["publisher_channel"] = None
                if retries >= max_retries:
                    raise RuntimeError(f"Failed to publish message after {max_retries} attempts") from e

                time.sleep(2 ** retries)
            except Exception as e:
                logger.error("Unexpected error while publishing message: %s", e)
                raise RuntimeError(f"Unexpected error while publishing message: {e}") from e

    def stop(self):
        """Stop the MQPipeline gracefully."""
        logger.info("Stopping MQPipeline...")
        self._internal_threads["stop_event"].set()  # Signal threads to stop
        self._internal_threads["consumer_thread"].join(timeout=5)
        self._internal_threads["subscriber_thread"].join(timeout=5)
        with self._publisher_conninfo_lock:
            publisher_connection = self._publisher_conninfo.get("publisher_connection")
            if publisher_connection and not publisher_connection.is_closed:
                try:
                    publisher_channel = self._publisher_conninfo["publisher_channel"]
                    if publisher_channel and not publisher_channel.is_closed:
                        publisher_channel.close()
                        logger.info("Publisher channel closed.")
                except Exception as e: #pylint: disable=broad-except
                    logger.error("Error closing publisher channel: %s", e)

                try:
                    publisher_connection.close()
                    logger.info("Publisher connection closed.")
                except Exception as e: #pylint: disable=broad-except
                    logger.error("Error closing publisher connection: %s", e)

            self._publisher_conninfo["publisher_connection"] = None
            self._publisher_conninfo["publisher_channel"] = None
        # Clear internal queues
        for q in self._internal_queues.values():
            while not q.empty():
                try:
                    q.get_nowait()
                    q.task_done()
                except q.Empty:
                    break

        logger.info("MQPipeline stopped.")

    def _setup_subscriber_channel(self, channel: BlockingConnection.channel, queue_name: str) -> BlockingConnection.channel:
        """Set up the subscriber channel with queue declaration and consumption."""
        channel.queue_declare(queue=queue_name, durable=True, arguments={"x-queue-type": "quorum"})
        channel.exchange_declare(
            exchange=self._config.subscriber_exchange,
            exchange_type='direct',
            durable=True
        )
        channel.queue_bind(
            queue=queue_name,
            exchange=self._config.subscriber_exchange,
            routing_key=self._config.subscriber_routing_key
        )
        channel.basic_consume(queue=queue_name, on_message_callback=self._callback, auto_ack=False)
        return channel

    def start(self):
        """Start the MQPipeline in a non-blocking manner."""
        subscriber_thread = self._internal_threads["subscriber_thread"]
        consumer_thread = self._internal_threads["consumer_thread"]
        stop_event = self._internal_threads["stop_event"]
        if subscriber_thread and subscriber_thread.is_alive():
            logger.warning("MQPipeline is already running.")
            return
        stop_event.clear()  # Reset stop event
        consumer_thread.start()  # Start processing thread
        subscriber_thread.start()
        logger.info("MQPipeline started.")

    def join(self, timeout=None):
        """Wait for the MQPipeline to shut down gracefully."""
        logger.info("Waiting for MQPipeline to complete...")
        self._internal_threads["stop_event"].wait() # Wait for stop event to be set
        for thread_name, thread in self._internal_threads.items():
            if thread_name != "stop_event" and thread.is_alive():
                thread.join(timeout=timeout or 15)
                if thread.is_alive():
                    logger.warning("Thread %s still running after timeout.", thread_name)
        logger.info("MQPipeline shutdown complete.")

    def _run_consumer(self):
        """Run the consumer loop in a separate thread."""
        try:
            subscribe_url = f"amqp://{self._config.mq_user}:{self._config.mq_password}@{self._config.mq_host}/{self._config.mq_vhost}"
            connection, subscriber_channel = self._connect_rabbitmq(subscribe_url, self._config.subscriber_queue)
            subscriber_channel = self._setup_subscriber_channel(subscriber_channel, self._config.subscriber_queue)
            logger.info("Starting to consume messages from queue: %s", self._config.subscriber_queue)
            while not self._internal_threads["stop_event"].is_set():
                try:
                    self._poll_ack_queue(subscriber_channel)
                    connection.process_data_events(time_limit=0.05)  # Reduced for faster shutdown
                except (ConnectionClosedByBroker, AMQPConnectionError, ChannelClosedByBroker, ChannelWrongStateError) as e:
                    logger.error("Connection error while consuming messages: %s", e, exc_info=True)
                    logger.info("Reconnecting to RabbitMQ...")
                    time.sleep(5)
                    try:
                        connection.close()
                    except Exception: #pylint: disable=broad-except
                        logger.error("Error closing connection during reconnection attempt", exc_info=True)
                    connection, subscriber_channel = self._connect_rabbitmq(subscribe_url, self._config.subscriber_queue)
                    subscriber_channel = self._setup_subscriber_channel(subscriber_channel, self._config.subscriber_queue)
                except Exception as e:
                    logger.error("Unexpected error while consuming messages: %s", e, exc_info=True)
                    raise RuntimeError(f"Unexpected error while consuming messages: {e}") from e
        except Exception as e:
            logger.error("Failed to subscribe to queue: %s", e, exc_info=True)
            raise RuntimeError(f"Failed to subscribe to queue: {e}") from e
        finally:
            try:
                connection.close()
                logger.info("Subscriber connection closed.")
            except Exception as e: #pylint: disable=broad-except
                logger.error("Error closing subscriber connection: %s", e, exc_info=True)

    def _callback(self, ch, method, properties, body):
        """Callback function to handle incoming messages."""
        logger.info("Received message with delivery_tag %s on channel %s", method.delivery_tag, id(ch))
        self._internal_queues["processing_queue"].put((ch, method, properties, body))
        logger.info("Message added to processing queue")

    def _poll_ack_queue(self, channel):
        """Poll the acknowledgment queue and process acknowledgments."""
        while True:
            try:
                channel, delivery_tag, action = self._internal_queues["ack_queue"].get(timeout=1)
                logger.debug("Processing ack for delivery_tag %s with action %s", delivery_tag, action)
                if action == "ack":
                    channel.basic_ack(delivery_tag=delivery_tag)
                    logger.debug("Acknowledged message with delivery_tag %s", delivery_tag)
                elif action == "reject":
                    channel.basic_reject(delivery_tag=delivery_tag, requeue=True)
                    logger.debug("Rejected message with delivery_tag %s", delivery_tag)
                self._internal_queues["ack_queue"].task_done()
            except queue.Empty:
                break
            except Exception as e: #pylint: disable=broad-except
                logger.error("Error in ack handler: %s", e)

    def _process_messages(self):
        """Process messages from the processing queue."""
        while not self._internal_threads["stop_event"].is_set():
            try:
                ch, method, properties, body = self._internal_queues["processing_queue"].get(timeout=0.1)
                logger.info("Processing message with delivery_tag %s and properties %s", method.delivery_tag, properties)
                try:
                    if self._single_message_handler(body, self._publish):
                        logger.info("Message processed successfully, acknowledging...")
                        self._internal_queues["ack_queue"].put((ch, method.delivery_tag, "ack"))
                    else:
                        logger.info("Message processing failed, rejecting...")
                        self._internal_queues["ack_queue"].put((ch, method.delivery_tag, "reject"))
                except Exception as e: #pylint: disable=broad-except
                    logger.error("Error processing message: %s, rejecting...", e)
                    self._internal_queues["ack_queue"].put((ch, method.delivery_tag, "reject"))
                finally:
                    self._internal_queues["processing_queue"].task_done()
            except queue.Empty:
                continue
            except Exception as e: #pylint: disable=broad-except
                logger.error("Unexpected error while processing messages: %s", e)
                raise RuntimeError(f"Unexpected error while processing messages: {e}") from e
