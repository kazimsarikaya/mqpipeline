import os
from mqpipeline.config import MQPipelineConfig
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

config = MQPipelineConfig.from_env_keys({"exchange": "MQ_EXCHANGE2",})

logger.debug(f"MQPipelineConfig: {config}")


import time
time.sleep(999999999)  # Simulate long-running process
