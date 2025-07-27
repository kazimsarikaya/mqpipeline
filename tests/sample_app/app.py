import signal
import sys
import os
import logging
import time

from mqpipeline.config import MQPipelineConfig
from mqpipeline.pipeline import MQPipeline

logging.basicConfig(level=logging.DEBUG,)

pika_logger = logging.getLogger('pika')
pika_logger.setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# Config
config = MQPipelineConfig.from_env_keys({"exchange": "MQ_EXCHANGE2",})
logger.debug(f"MQPipelineConfig: {config}")

def handle_single_message(message,publish):
    """Handle a single message from the queue."""
    logger.info(f"Processing message: {message}")
    # Simulate message processing
    # In a real application, you would add your message processing logic here
    time.sleep(30)  # Simulate processing delay
    publish(b"Processed message: " + message)
    return True # Return True to acknowledge the message False to reject it

# Initialize the MQPipeline with the configuration and message handler
pipeline = MQPipeline(
    config=config,
    single_message_handler=handle_single_message
)

# Handle SIGTERM properly
def handle_sigterm(signum, frame):
    logger.info("Received SIGTERM, shutting down gracefully...")
    pipeline.stop()
    exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

# Start the pipeline
pipeline.start()

# Keep the application running to process messages
pipeline.join()
