"""MQPipeline Example Application
This example application demonstrates how to use the MQPipeline to process messages from a message queue.
It includes handling messages, publishing results, and managing errors.

Sample mq message format:
{
    "sleep_time": 2,  # Time in seconds to simulate processing
    "produce_error": false  # Set to true to simulate an error
}
or
{
    "sleep_time": 3, # Time in seconds to simulate processing
    "produce_error": true  # Set to true to simulate an error
}

You can send messages at rabbitmq management UI or using a tool like `curl` or `httpie`.

MQPipelineConfig.from_env_keys() is used to load configuration from environment variables.
it also provides custom environment keys. Please look at the `config.py` file for more details.
"""

import signal
import sys
import logging
import time
import json

from mqpipeline.config import MQPipelineConfig
from mqpipeline.pipeline import MQPipeline

logging.basicConfig(level=logging.DEBUG,)

pika_logger = logging.getLogger('pika')
pika_logger.setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# Config
config = MQPipelineConfig.from_env_keys()
logger.debug("MQPipelineConfig: %s", config)

def handle_single_message(message,publish, publish_error=None):
    """Handle a single message from the queue."""
    # Simulate processing the message
    logger.info("Processing message: %s", message)
    data = json.loads(message.decode('utf-8'))

    sleep_time = data.get('sleep_time', 0)

    time.sleep(sleep_time)  # Simulate processing time

    pub_msg = {
        "status": "success",
        "message": f"Processed message with sleep time {sleep_time}",
        "received_data": data
    }

    err_msg = {
        "status": "error",
        "message": f"Failed to process message with sleep time {sleep_time}",
        "received_data": data
    }

    if data.get('produce_error', False):
        logger.error("Error processing message: %s", message)
        if publish_error:
            logger.error("Publishing error message: %s", err_msg)
            publish_error(json.dumps(err_msg).encode('utf-8'))
            # in this case we handled message however it has error, we send it to error queue
            # hence we also act as if we acknowledged the message
        else:
            logger.error("No error handler provided, cannot publish error message.")
            return False # Reject the message if error handling is not configured do not lose it
    else:
        logger.info("Successfully processed message: %s", message)
        # Publish the processed message
        publish(json.dumps(pub_msg).encode('utf-8'))

    return True # Return True to acknowledge the message False to reject it

# Initialize the MQPipeline with the configuration and message handler
pipeline = MQPipeline(
    config=config,
    single_message_handler=handle_single_message
)

# Handle SIGTERM properly
def handle_sigterm(_signum, _frame):
    """Handle SIGTERM signal for graceful shutdown."""
    logger.info("Received SIGTERM, shutting down gracefully...")
    pipeline.stop()
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

# Start the pipeline
pipeline.start()

# Keep the application running to process messages
pipeline.join()
