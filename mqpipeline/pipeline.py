"""MQPipeline class for managing RabbitMQ message queue operations.

This module defines the `MQPipeline` class, which handles publishing and subscribing to
RabbitMQ message queues. It uses a configuration from `MQPipelineConfig` to connect to
RabbitMQ, process messages with a user-provided handler, and support error queues for failed
messages. The class runs two threads: one for consuming messages from RabbitMQ and another
for processing them, ensuring non-blocking operation.

Example:
    To use `MQPipeline`, create a configuration and a message handler, then start the pipeline:

    .. code-block:: python

        from mqpipeline.config import MQPipelineConfig
        from mqpipeline.pipeline import MQPipeline

        # Configure environment variables (or use a .env file)
        import os
        os.environ.update({
            "MQ_HOST": "rabbitmq",
            "MQ_USER": "guest",
            "MQ_PASSWORD": "guest",
            "MQ_VHOST": "/",
            "MQ_APPLICATION": "my_app",
            "MQ_PUBLISHER_EXCHANGE": "pub_ex",
            "MQ_PUBLISHER_QUEUE": "pub_queue",
            "MQ_PUBLISHER_ROUTING_KEY": "pub_key",
            "MQ_SUBSCRIBER_EXCHANGE": "sub_ex",
            "MQ_SUBSCRIBER_QUEUE": "sub_queue",
            "MQ_SUBSCRIBER_ROUTING_KEY": "sub_key",
            "MQ_HAS_ERROR_QUEUE": "true",
            "MQ_ERROR_EXCHANGE": "err_ex",
            "MQ_ERROR_QUEUE": "err_queue",
            "MQ_ERROR_ROUTING_KEY": "err_key"
        })

        # Define a message handler
        def handle_message(message, publish, publish_error):
            print(f"Received: {message.decode()}")
            publish(b"Processed: " + message)
            return True  # Acknowledge the message

        # Create and start the pipeline
        config = MQPipelineConfig.from_env_keys()
        pipeline = MQPipeline(config=config, single_message_handler=handle_message)
        pipeline.start()
        pipeline.join()  # Wait for shutdown (e.g., via SIGTERM)
"""

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
    """Manages RabbitMQ message queue operations for publishing and subscribing.

    The `MQPipeline` class connects to a RabbitMQ server using settings from
    `MQPipelineConfig`. It runs two threads:

    - A subscriber thread that consumes messages from a RabbitMQ queue and puts them into an internal processing queue.
    - A consumer thread that processes messages using a user-provided handler function.

    It supports publishing messages to a publisher queue and, if enabled, an error queue
    for failed messages. The class handles connection errors with retries and ensures
    graceful shutdown on SIGTERM or SIGINT.

    Attributes
    ----------
    _config : MQPipelineConfig
        Configuration object with RabbitMQ and queue settings.
    _single_message_handler : callable
        User-provided function to process messages.
        It takes three arguments: the message (bytes), a publish function, and an
        optional publish_error function.
    _internal_queues : dict
        Internal queues for processing and acknowledging messages.
        Contains "processing_queue" (Queue) and "ack_queue" (Queue).
    _internal_threads : dict
        Threads and events for managing the pipeline. Contains
        "stop_event" (Event), "consumer_thread" (Thread), and "subscriber_thread" (Thread).
    _publisher_conninfo_lock : Lock
        Thread lock for synchronizing access to publisher and error queue connections.
    _publisher_conninfo : dict
        Connection and channel information for publisher and error queues.
        Contains "publisher_connection", "publisher_channel",
        "error_connection", and "error_channel".

    Example
    -------
    Create a pipeline with an error queue and custom handler::

        def handle_message(message, publish, publish_error):
            try:
                print(f"Processing: {message.decode()}")
                publish(b"Success: " + message)
                return True
            except Exception:
                publish_error(b"Failed: " + message)
                return False

        config = MQPipelineConfig(
            mq_host="localhost",
            mq_user="guest",
            mq_password="guest",
            mq_vhost="/",
            mq_application="test_app",
            publisher_exchange="pub_ex",
            publisher_queue="pub_queue",
            publisher_routing_key="pub_key",
            subscriber_exchange="sub_ex",
            subscriber_queue="sub_queue",
            subscriber_routing_key="sub_key",
            mq_has_error_queue=True,
            error_exchange="err_ex",
            error_queue="err_queue",
            error_routing_key="err_key"
        )
        pipeline = MQPipeline(config=config, single_message_handler=handle_message)
        pipeline.start()
    """

    def __init__(self, config: MQPipelineConfig, single_message_handler: callable):
        """Initialize the MQPipeline with configuration and message handler.

        Sets up internal queues, threads, and locks for managing RabbitMQ connections
        and message processing. The pipeline is not started until `start()` is called.

        Args:
            config (MQPipelineConfig): Configuration object with RabbitMQ and queue settings.
            single_message_handler (callable): Function to process a single message.
                It takes three arguments:
                - message (bytes): The message received from the subscriber queue.
                - publish (callable): Function to publish a message to the publisher queue.
                - publish_error (callable | None): Function to publish a message to the error
                  queue, or None if error queue is disabled.
                The handler should return True to acknowledge the message or False to reject it.

        Example:
            .. code-block:: python

                def my_handler(message, publish, publish_error):
                    print(f"Got message: {message.decode()}")
                    publish(b"Processed: " + message)
                    return True

                config = MQPipelineConfig.from_env_keys()
                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
        """
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
            "error_connection": None,
            "error_channel": None
        }

    def _connect_rabbitmq(self, url: str, queue_name: str):
        """Establish a connection to RabbitMQ for a specific queue.

        Creates a connection and channel to RabbitMQ using the provided URL and configures
        connection parameters like heartbeat and retry settings. The connection name includes
        the application name, queue name, and hostname for debugging.

        Args:
            url (str): AMQP URL for RabbitMQ (e.g., "amqp://guest:guest@localhost/").
            queue_name (str): Name of the queue for the connection (e.g., "pub_queue").

        Returns:
            tuple: A tuple of (connection, channel), where:
                - connection (BlockingConnection): The RabbitMQ connection.
                - channel (BlockingConnection.channel): The RabbitMQ channel.

        Raises:
            RuntimeError: If the connection fails due to network issues or invalid credentials.

        Example:
            .. code-block:: python

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                url = "amqp://guest:guest@localhost/"
                conn, chan = pipeline._connect_rabbitmq(url, "pub_queue")
                print(f"Connected: {conn.is_open}")  # Outputs: Connected: True
        """
        try:
            logger.debug("Connecting to RabbitMQ at %s for queue %s", url, queue_name)
            parameters = URLParameters(url)
            if parameters.client_properties is None:
                parameters.client_properties = {}

            parameters.client_properties["connection_name"] = f"{self._config.mq_application}-{queue_name}-{self._config.mq_client_hostname}"
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
        """Ensure an active publisher connection and channel to RabbitMQ.

        Checks if the publisher connection exists and is open. If not, it creates a new
        connection and channel, declares the publisher queue and exchange, and binds them
        with the routing key. Uses a lock to prevent concurrent access.

        Example:
            .. code-block:: python

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                pipeline._ensure_publisher_connection()
                # Publisher queue and exchange are now set up and ready for publishing
        """
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
        """Publish a message to the configured publisher queue.

        Sends a message to the RabbitMQ publisher exchange with the specified routing key.
        Retries up to three times on connection errors, with exponential backoff. Messages
        are marked as persistent to ensure they survive RabbitMQ restarts.

        Args:
            message (bytes): The message to publish (must be bytes, not a string).

        Raises:
            RuntimeError: If publishing fails after three retries or due to an unexpected error.

        Example:
            .. code-block:: python

                def my_handler(message, publish, publish_error):
                    publish(b"Processed: " + message)
                    return True

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                pipeline._publish(b"Hello, RabbitMQ!")
                # Message is published to pub_ex with routing key pub_key
        """
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

    def _ensure_error_connection(self):
        """Ensure an active error queue connection and channel to RabbitMQ.

        Checks if the error queue connection exists and is open (if `mq_has_error_queue` is True).
        If not, it creates a new connection and channel, declares the error queue and exchange,
        and binds them with the routing key. Uses a lock to prevent concurrent access.

        Example:
            .. code-block:: python

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                pipeline._ensure_error_connection()
                # Error queue and exchange are set up if mq_has_error_queue is True
        """
        with self._publisher_conninfo_lock:
            error_connection = self._publisher_conninfo.get("error_connection")
            error_channel = self._publisher_conninfo.get("error_channel")
            if not error_connection or error_connection.is_closed:
                url = f"amqp://{self._config.mq_user}:{self._config.mq_password}@{self._config.mq_host}/{self._config.mq_vhost}"
                error_connection, error_channel = self._connect_rabbitmq(url, self._config.error_queue)
                # create queue if it does not exist with durable and quorum settings
                error_channel.queue_declare(
                    queue=self._config.error_queue,
                    durable=True,
                    arguments={
                        "x-queue-type": "quorum"
                    }
                )
                # Ensure the exchange exists
                error_channel.exchange_declare(
                    exchange=self._config.error_exchange,
                    exchange_type='direct',
                    durable=True
                )
                # Bind the queue to the exchange with the routing key
                error_channel.queue_bind(
                    queue=self._config.error_queue,
                    exchange=self._config.error_exchange,
                    routing_key=self._config.error_routing_key
                )

                self._publisher_conninfo["error_connection"] = error_connection
                self._publisher_conninfo["error_channel"] = error_channel

    def _publish_error(self, message):
        """Publish a message to the configured error queue.

        Sends a message to the RabbitMQ error exchange with the specified routing key (if
        `mq_has_error_queue` is True). Retries up to three times on connection errors, with
        exponential backoff. Messages are marked as persistent.

        Args:
            message (bytes): The message to publish to the error queue (must be bytes).

        Raises:
            RuntimeError: If publishing fails after three retries or due to an unexpected error.

        Example:
            .. code-block:: python

                def my_handler(message, publish, publish_error):
                    if b"error" in message:
                        publish_error(b"Error: " + message)
                        return False
                    return True

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                pipeline._publish_error(b"Failed message")
                # Message is published to err_ex with routing key err_key
        """
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                self._ensure_error_connection()
                error_channel = self._publisher_conninfo["error_channel"]
                error_channel.basic_publish(
                    exchange=self._config.error_exchange,
                    routing_key=self._config.error_routing_key,
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
                self._publisher_conninfo["error_connection"] = None
                self._publisher_conninfo["error_channel"] = None
                if retries >= max_retries:
                    raise RuntimeError(f"Failed to publish error message after {max_retries} attempts") from e

                time.sleep(2 ** retries)
            except Exception as e:
                logger.error("Unexpected error while publishing error message: %s", e)
                raise RuntimeError(f"Unexpected error while publishing error message: {e}") from e

    def stop(self):
        """Stop the MQPipeline gracefully, closing connections and threads.

        Signals the subscriber and consumer threads to stop, closes RabbitMQ connections
        and channels (publisher and error queues), and clears internal queues. Ensures a
        clean shutdown, typically triggered by SIGTERM or SIGINT.

        Example:
            .. code-block:: python

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                pipeline.start()
                pipeline.stop()  # Gracefully shuts down threads and connections
        """
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
                except Exception as e:  # pylint: disable=broad-except
                    logger.error("Error closing publisher channel: %s", e)

                try:
                    publisher_connection.close()
                    logger.info("Publisher connection closed.")
                except Exception as e:  # pylint: disable=broad-except
                    logger.error("Error closing publisher connection: %s", e)

            error_connection = self._publisher_conninfo.get("error_connection")
            if error_connection and not error_connection.is_closed:
                try:
                    error_channel = self._publisher_conninfo["error_channel"]
                    if error_channel and not error_channel.is_closed:
                        error_channel.close()
                        logger.info("Error channel closed.")
                except Exception as e:  # pylint: disable=broad-except
                    logger.error("Error closing error channel: %s", e)

                try:
                    error_connection.close()
                    logger.info("Error connection closed.")
                except Exception as e:  # pylint: disable=broad-except
                    logger.error("Error closing error connection: %s", e)

            self._publisher_conninfo["publisher_connection"] = None
            self._publisher_conninfo["publisher_channel"] = None
            self._publisher_conninfo["error_connection"] = None
            self._publisher_conninfo["error_channel"] = None
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
        """Configure the subscriber channel for consuming messages.

        Declares the subscriber queue and exchange, binds them with the routing key, and
        sets up a callback for incoming messages. The queue is durable and uses quorum settings.

        Args:
            channel (BlockingConnection.channel): The RabbitMQ channel to configure.
            queue_name (str): The name of the subscriber queue (e.g., "sub_queue").

        Returns:
            BlockingConnection.channel: The configured channel ready for consumption.

        Example:
            .. code-block:: python

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                conn, chan = pipeline._connect_rabbitmq("amqp://guest:guest@localhost/", "sub_queue")
                chan = pipeline._setup_subscriber_channel(chan, "sub_queue")
                # Channel is ready to consume messages
        """
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
        """Start the MQPipeline in a non-blocking manner.

        Starts the subscriber and consumer threads to begin consuming and processing
        messages from the subscriber queue. Resets the stop event to allow operation.
        If the pipeline is already running, it logs a warning and returns.

        Example:
            .. code-block:: python

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                pipeline.start()  # Starts consuming and processing messages
        """
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
        """Wait for the MQPipeline to shut down gracefully.

        Blocks until the stop event is set (e.g., by calling `stop()` or receiving SIGTERM)
        and all threads have completed. Each thread is given a timeout (default 15 seconds)
        to finish, with a warning if it exceeds the timeout.

        Args:
            timeout (float, optional): Maximum time to wait for each thread to finish (in seconds).
                If None, defaults to 15 seconds per thread.

        Example:
            .. code-block:: python

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                pipeline.start()
                # Simulate SIGTERM or manual stop
                pipeline.stop()
                pipeline.join(timeout=10)  # Wait up to 10 seconds for shutdown
        """
        logger.info("Waiting for MQPipeline to complete...")
        self._internal_threads["stop_event"].wait()  # Wait for stop event to be set
        for thread_name, thread in self._internal_threads.items():
            if thread_name != "stop_event" and thread.is_alive():
                thread.join(timeout=timeout or 15)
                if thread.is_alive():
                    logger.warning("Thread %s still running after timeout.", thread_name)
        logger.info("MQPipeline shutdown complete.")

    def _run_consumer(self):
        """Run the consumer loop to fetch messages from RabbitMQ.

        Runs in a separate thread to consume messages from the subscriber queue, passing
        them to the processing queue via the `_callback` method. Handles connection errors
        with reconnection attempts and ensures the connection is closed on shutdown.

        Raises:
            RuntimeError: If subscribing to the queue fails due to persistent errors.

        Example:
            .. code-block:: python

                pipeline = MQPipeline(config=config, single_message_handler=my_handler)
                # Normally called by start(), but can be tested directly
                pipeline._run_consumer()  # Starts consuming messages
        """
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
                    except Exception:  # pylint: disable=broad-except
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
            except Exception as e:  # pylint: disable=broad-except
                logger.error("Error closing subscriber connection: %s", e, exc_info=True)

    def _callback(self, ch, method, properties, body):
        """Handle incoming RabbitMQ messages and add them to the processing queue.

        Called by RabbitMQ when a message is received. Adds the message and its metadata
        to the internal processing queue for the consumer thread to handle.

        Args:
            ch (BlockingConnection.channel): The RabbitMQ channel delivering the message.
            method (pika.spec.Basic.Deliver): Delivery metadata (e.g., delivery_tag).
            properties (pika.spec.BasicProperties): Message properties (e.g., delivery_mode).
            body (bytes): The message content.

        Example:
            .. code-block:: python

                # Normally called by RabbitMQ, but can be tested directly
                pipeline._callback(channel, method, properties, b"Hello")
                # Message is added to processing_queue for processing
        """
        logger.info("Received message with delivery_tag %s on channel %s", method.delivery_tag, id(ch))
        self._internal_queues["processing_queue"].put((ch, method, properties, body))
        logger.info("Message added to processing queue")

    def _poll_ack_queue(self, channel):
        """Process acknowledgments and rejections from the acknowledgment queue.

        Polls the acknowledgment queue to acknowledge or reject messages processed by
        the consumer thread. Runs in the subscriber thread to ensure thread safety.

        Args:
            channel (BlockingConnection.channel): The RabbitMQ channel to send acks/rejects.

        Example:
            .. code-block:: python

                pipeline._poll_ack_queue(channel)
                # Processes any pending acks or rejects in ack_queue
        """
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
            except Exception as e:  # pylint: disable=broad-except
                logger.error("Error in ack handler: %s", e)

    def _process_messages(self):
        """Process messages from the internal processing queue.

        Runs in a separate thread to process messages using the user-provided handler.
        Messages are acknowledged if the handler returns True, or rejected (requeued)
        if False. Supports publishing to an error queue if enabled.

        Raises:
            RuntimeError: If an unexpected error occurs during message processing.

        Example:
            .. code-block:: python

                # Normally called by consumer_thread, but can be tested directly
                pipeline._process_messages()
                # Processes messages from processing_queue
        """
        while not self._internal_threads["stop_event"].is_set():
            try:
                ch, method, properties, body = self._internal_queues["processing_queue"].get(timeout=0.1)
                logger.info("Processing message with delivery_tag %s and properties %s", method.delivery_tag, properties)
                try:
                    publish_error = None
                    if self._config.mq_has_error_queue:
                        publish_error = self._publish_error
                    if self._single_message_handler(body, self._publish, publish_error):
                        logger.info("Message processed successfully, acknowledging...")
                        self._internal_queues["ack_queue"].put((ch, method.delivery_tag, "ack"))
                    else:
                        logger.info("Message processing failed, rejecting...")
                        self._internal_queues["ack_queue"].put((ch, method.delivery_tag, "reject"))
                except Exception as e:  # pylint: disable=broad-except
                    logger.error("Error processing message: %s, rejecting...", e)
                    self._internal_queues["ack_queue"].put((ch, method.delivery_tag, "reject"))
                finally:
                    self._internal_queues["processing_queue"].task_done()
            except queue.Empty:
                continue
            except Exception as e:  # pylint: disable=broad-except
                logger.error("Unexpected error while processing messages: %s", e)
                raise RuntimeError(f"Unexpected error while processing messages: {e}") from e
