"""Configuration for MQPipeline using Pydantic and environment variables."""

import os

from typing import Mapping

from pydantic import Field
from pydantic_settings import BaseSettings

class MQPipelineConfig(BaseSettings):
    """Configuration for MQPipeline."""

    # Shared system-wide MQ config
    mq_application: str = Field("application", env="MQ_APPLICATION")
    mq_host: str = Field(..., env="MQ_HOST")
    mq_vhost: str = Field("", env="MQ_VHOST")
    mq_user: str = Field(..., env="MQ_USER")
    mq_password: str = Field(..., env="MQ_PASSWORD")
    mq_fetch_count: int = Field(1, env="MQ_FETCH_COUNT")

    # App-specific keys (optional, with defaults from env or empty strings)
    publisher_exchange: str = Field(..., env="MQ_PUBLISHER_EXCHANGE")
    publisher_queue: str = Field(..., env="MQ_PUBLISHER_QUEUE")
    publisher_routing_key: str = Field(..., env="MQ_PUBLISHER_ROUTING_KEY")
    subscriber_exchange: str = Field(..., env="MQ_SUBSCRIBER_EXCHANGE")
    subscriber_queue: str = Field(..., env="MQ_SUBSCRIBER_QUEUE")
    subscriber_routing_key: str = Field(..., env="MQ_SUBSCRIBER_ROUTING_KEY")

    class Config: #pylint: disable=too-few-public-methods
        """Pydantic configuration."""
        env_file_encoding = "utf-8"

    @classmethod
    def from_env_keys(cls, env_keys: Mapping[str, str] = None):
        """Create an instance of MQPipelineConfig from environment variables."""
        default_keys = {
            "publisher_exchange": "MQ_PUBLISHER_EXCHANGE",
            "publisher_queue": "MQ_PUBLISHER_QUEUE",
            "publisher_routing_key": "MQ_PUBLISHER_ROUTING_KEY",
            "subscriber_exchange": "MQ_SUBSCRIBER_EXCHANGE",
            "subscriber_queue": "MQ_SUBSCRIBER_QUEUE",
            "subscriber_routing_key": "MQ_SUBSCRIBER_ROUTING_KEY",
        }
        env_keys = {**default_keys, **(env_keys or {})}
        env_overrides = {
            "publisher_exchange": cls._get_required_env(env_keys["publisher_exchange"]),
            "publisher_queue": cls._get_required_env(env_keys["publisher_queue"]),
            "publisher_routing_key": cls._get_required_env(env_keys["publisher_routing_key"]),
            "subscriber_exchange": cls._get_required_env(env_keys["subscriber_exchange"]),
            "subscriber_queue": cls._get_required_env(env_keys["subscriber_queue"]),
            "subscriber_routing_key": cls._get_required_env(env_keys["subscriber_routing_key"]),
        }
        return cls(**env_overrides)

    @staticmethod
    def _get_required_env(key: str) -> str:
        """Get a required environment variable, raising an error if not found."""
        value = os.getenv(key)
        if not value:
            raise RuntimeError(f"Missing required environment variable: {key}")
        return value
