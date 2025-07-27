"""Configuration for MQPipeline using Pydantic and environment variables."""

import os
import sys
from typing import Mapping

from pydantic import Field, computed_field
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
    mq_has_error_queue: bool = Field(False, env="MQ_HAS_ERROR_QUEUE")
    error_exchange: str | None = Field(None, env="MQ_ERROR_EXCHANGE")
    error_queue: str | None = Field(None, env="MQ_ERROR_QUEUE")
    error_routing_key: str | None = Field(None, env="MQ_ERROR_ROUTING_KEY")

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
            "mq_has_error_queue": "MQ_HAS_ERROR_QUEUE",
            "error_exchange": "MQ_ERROR_EXCHANGE",
            "error_queue": "MQ_ERROR_QUEUE",
            "error_routing_key": "MQ_ERROR_ROUTING_KEY",
        }

        env_keys = {**default_keys, **(env_keys or {})}

        mq_has_error_queue = os.getenv("MQ_HAS_ERROR_QUEUE", "False").lower() in ("true", "1", "yes")

        env_overrides = {
            "publisher_exchange": cls._get_required_env(env_keys["publisher_exchange"]),
            "publisher_queue": cls._get_required_env(env_keys["publisher_queue"]),
            "publisher_routing_key": cls._get_required_env(env_keys["publisher_routing_key"]),
            "subscriber_exchange": cls._get_required_env(env_keys["subscriber_exchange"]),
            "subscriber_queue": cls._get_required_env(env_keys["subscriber_queue"]),
            "subscriber_routing_key": cls._get_required_env(env_keys["subscriber_routing_key"]),
            "mq_has_error_queue": mq_has_error_queue,
        }

        if mq_has_error_queue:
            env_overrides.update({
                "error_exchange": cls._get_required_env(env_keys["error_exchange"]),
                "error_queue": cls._get_required_env(env_keys["error_queue"]),
                "error_routing_key": cls._get_required_env(env_keys["error_routing_key"]),
            })
        else:
            env_overrides.update({
                "error_exchange": None,
                "error_queue": None,
                "error_routing_key": None,
            })

        return cls(**env_overrides)

    @staticmethod
    def _get_required_env(key: str) -> str:
        """Get a required environment variable, raising an error if not found."""
        value = os.getenv(key)
        if not value:
            raise RuntimeError(f"Missing required environment variable: {key}")
        return value

    @computed_field
    @property
    def mq_client_hostname(self) -> str:
        """ Return local server name stripped of possible domain part.

        :return: Server name in lower case.
        """
        name = ('COMPUTERNAME' if sys.platform == 'win32' else 'HOSTNAME')
        return os.getenv(name, "unknown").upper().split('.')[0].lower()
