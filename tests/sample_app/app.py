"""MQPipeline Example Application.

This application demonstrates how to use the `MQPipeline` class to process messages
from a RabbitMQ message queue. It loads configuration from environment variables using
`MQPipelineConfig`, processes JSON messages with a custom handler, and supports publishing
results or errors to configured queues. The application handles graceful shutdown on
SIGTERM or SIGINT signals.

The expected message format is a JSON object with:
- `sleep_time` (int): Number of seconds to simulate processing (e.g., 2).
- `produce_error` (bool): If True, simulates an error and sends the message to the error queue.

Example:
    To run the application, set environment variables in a `.env` file or shell:

    .. code-block:: bash

        export MQ_HOST=rabbitmq
        export MQ_USER=guest
        export MQ_PASSWORD=guest
        export MQ_APPLICATION=my_app
        export MQ_PUBLISHER_EXCHANGE=pub_ex
        export MQ_PUBLISHER_QUEUE=pub_queue
        export MQ_PUBLISHER_ROUTING_KEY=pub_key
        export MQ_SUBSCRIBER_EXCHANGE=sub_ex
        export MQ_SUBSCRIBER_QUEUE=sub_queue
        export MQ_SUBSCRIBER_ROUTING_KEY=sub_key
        export MQ_HAS_ERROR_QUEUE=true
        export MQ_ERROR_EXCHANGE=err_ex
        export MQ_ERROR_QUEUE=err_queue
        export MQ_ERROR_ROUTING_KEY=err_key

    Run the application:

    .. code-block:: python

        python app.py

    Send a test message using the RabbitMQ management UI or a tool like `curl`:

    .. code-block:: bash

        curl -X POST http://guest:guest@localhost:15672/api/exchanges/%2F/sub_ex/publish \
            -H "Content-Type: application/json" \
            -d '{
                "routing_key": "sub_key",
                "payload": "{\"sleep_time\": 2, \"produce_error\": false}",
                "payload_encoding": "string",
                "properties": {"delivery_mode": 2}
            }'

    The application will process the message, publish a success message to `pub_queue`,
    and log the result.
"""

import signal
import sys
import logging
import time
import json

from mqpipeline.config import MQPipelineConfig
from mqpipeline.pipeline import MQPipeline

# Configure logging to show debug messages for the application
logging.basicConfig(level=logging.DEBUG)

# Reduce logging verbosity for the pika library to avoid excessive output
pika_logger = logging.getLogger('pika')
pika_logger.setLevel(logging.WARNING)

# Create a logger for this module
logger = logging.getLogger(__name__)

# Load configuration from environment variables using MQPipelineConfig
config = MQPipelineConfig.from_env_keys()
# Log the configuration for debugging
logger.debug("MQPipelineConfig: %s", config)

def handle_single_message(message, publish, publish_error=None):
    """Handle a single message from the subscriber queue.

    Processes a JSON message from the queue, simulating work by sleeping for the specified
    `sleep_time`. If `produce_error` is True and an error queue is configured, the message
    is sent to the error queue. Otherwise, a success message is published to the publisher
    queue. The function returns True to acknowledge the message or False to reject it.

    Args:
        message (bytes): The message received from the subscriber queue, expected to be a
            JSON-encoded string (e.g., b'{"sleep_time": 2, "produce_error": false}').
        publish (callable): Function to publish a message to the publisher queue. Takes a
            bytes argument (e.g., b'{"status": "success", ...}').
        publish_error (callable, optional): Function to publish a message to the error queue
            if `mq_has_error_queue` is True. Takes a bytes argument. Defaults to None.

    Returns:
        bool: True to acknowledge the message (processed successfully or sent to error queue),
            False to reject the message (error occurred and no error queue is configured).

    Example:
        Process a message that simulates an error:

        .. code-block:: python

            message = b'{"sleep_time": 3, "produce_error": true}'
            def my_publish(msg):
                print(f"Publishing: {msg.decode()}")
            def my_publish_error(msg):
                print(f"Error queue: {msg.decode()}")

            result = handle_single_message(message, my_publish, my_publish_error)
            # Logs error and publishes to error queue
            print(result)  # Outputs: True

        Process a successful message:

        .. code-block:: python

            message = b'{"sleep_time": 2, "produce_error": false}'
            result = handle_single_message(message, my_publish)
            # Publishes success message to publisher queue
            print(result)  # Outputs: True
    """
    # Log the received message for debugging
    logger.info("Processing message: %s", message)
    # Decode and parse the JSON message
    data = json.loads(message.decode('utf-8'))

    # Get sleep time from the message, default to 0 if not provided
    sleep_time = data.get('sleep_time', 0)

    # Simulate processing by sleeping for the specified duration
    time.sleep(sleep_time)

    # Prepare a success message to publish
    pub_msg = {
        "status": "success",
        "message": f"Processed message with sleep time {sleep_time}",
        "received_data": data
    }

    # Prepare an error message for failed processing
    err_msg = {
        "status": "error",
        "message": f"Failed to process message with sleep time {sleep_time}",
        "received_data": data
    }

    # Check if the message should simulate an error
    if data.get('produce_error', False):
        # Log the error condition
        logger.error("Error processing message: %s", message)
        if publish_error:
            # If error queue is configured, publish the error message
            logger.error("Publishing error message: %s", err_msg)
            publish_error(json.dumps(err_msg).encode('utf-8'))
            # Acknowledge the message since it was handled (sent to error queue)
            return True
        # If no error queue is configured, reject the message to avoid losing it
        logger.error("No error handler provided, cannot publish error message.")
        return False
    # If no error, publish the success message
    logger.info("Successfully processed message: %s", message)
    publish(json.dumps(pub_msg).encode('utf-8'))
    # Acknowledge the message since it was processed successfully
    return True

# Initialize the MQPipeline with the configuration and message handler
pipeline = MQPipeline(
    config=config,
    single_message_handler=handle_single_message
)

def handle_sigterm(_signum, _frame):
    """Handle SIGTERM signal to shut down the application gracefully.

    Called when the application receives a SIGTERM signal (e.g., from `podman kill` or
    a process manager). Stops the `MQPipeline` and exits the application cleanly.

    Args:
        _signum (int): The signal number (e.g., signal.SIGTERM).
        _frame (frame): The current stack frame (unused).

    Example:
        Simulate a SIGTERM signal:

        .. code-block:: python

            import signal
            handle_sigterm(signal.SIGTERM, None)
            # Logs "Received SIGTERM, shutting down gracefully..." and stops the pipeline
    """
    # Log the SIGTERM signal for debugging
    logger.info("Received SIGTERM, shutting down gracefully...")
    # Stop the pipeline to close connections and threads
    pipeline.stop()
    # Exit the application cleanly
    sys.exit(0)

# Register the SIGTERM handler to ensure graceful shutdown
signal.signal(signal.SIGTERM, handle_sigterm)

# Start the pipeline to begin consuming and processing messages
pipeline.start()

# Keep the application running, waiting for messages or shutdown
pipeline.join()
