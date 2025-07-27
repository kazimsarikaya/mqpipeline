"""Configuration for MQPipeline using Pydantic and environment variables.

This module defines the `MQPipelineConfig` class, which manages configuration settings
for the `MQPipeline` class. It uses Pydantic to load settings from environment variables
or a `.env` file, making it easy to configure RabbitMQ connections and queue settings.
The configuration supports both required and optional fields, with conditional logic for
error queue settings based on the `mq_has_error_queue` flag.

Example:
    To configure `MQPipeline`, set environment variables in a shell or a `.env` file:

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

    Then, create a config object in Python:

    .. code-block:: python

        from mqpipeline.config import MQPipelineConfig
        config = MQPipelineConfig.from_env_keys()
        print(config.mq_host)  # Outputs: rabbitmq
        print(config.mq_has_error_queue)  # Outputs: True
        print(config.error_exchange)  # Outputs: err_ex
"""

import os
import sys
from typing import Mapping

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings

class MQPipelineConfig(BaseSettings):
    """Configuration class for MQPipeline using Pydantic.

    This class defines settings for connecting to RabbitMQ and configuring message queues
    for publishing and subscribing. It uses Pydantic to load values from environment variables
    or a `.env` file. Fields are grouped into:
    - Shared RabbitMQ connection settings (e.g., host, user, password).
    - Publisher and subscriber queue settings (e.g., exchange, queue, routing key).
    - Optional error queue settings, enabled by `mq_has_error_queue`.

    Attributes:
        mq_application (str): Name of the application using MQPipeline. Identifies the
            application in logs and RabbitMQ connection names. Loaded from `MQ_APPLICATION`.
            Default: "application".
        mq_host (str): Hostname or IP address of the RabbitMQ server (e.g., "localhost" or
            "rabbitmq"). Required, loaded from `MQ_HOST`. No default.
        mq_vhost (str): RabbitMQ virtual host, a logical grouping of queues and exchanges
            (e.g., "/" or "my_vhost"). Loaded from `MQ_VHOST`. Default: empty string.
        mq_user (str): Username for RabbitMQ authentication. Required, loaded from `MQ_USER`.
            No default.
        mq_password (str): Password for RabbitMQ authentication. Required, loaded from
            `MQ_PASSWORD`. No default.
        mq_fetch_count (int): Number of messages to prefetch from the subscriber queue at
            once. Controls how many messages are fetched before processing. Loaded from
            `MQ_FETCH_COUNT`. Default: 1.
        publisher_exchange (str): Name of the RabbitMQ exchange for publishing messages.
            Exchanges route messages to queues. Required, loaded from `MQ_PUBLISHER_EXCHANGE`.
            No default.
        publisher_queue (str): Name of the RabbitMQ queue for publishing messages. Required,
            loaded from `MQ_PUBLISHER_QUEUE`. No default.
        publisher_routing_key (str): Routing key for binding the publisher queue to its
            exchange. Determines which messages go to the queue. Required, loaded from
            `MQ_PUBLISHER_ROUTING_KEY`. No default.
        subscriber_exchange (str): Name of the RabbitMQ exchange for subscribing to messages.
            Required, loaded from `MQ_SUBSCRIBER_EXCHANGE`. No default.
        subscriber_queue (str): Name of the RabbitMQ queue for receiving messages. Required,
            loaded from `MQ_SUBSCRIBER_QUEUE`. No default.
        subscriber_routing_key (str): Routing key for binding the subscriber queue to its
            exchange. Required, loaded from `MQ_SUBSCRIBER_ROUTING_KEY`. No default.
        mq_has_error_queue (bool): Whether to enable an error queue for failed messages.
            If True, error queue fields are required; if False, they are optional. Loaded
            from `MQ_HAS_ERROR_QUEUE` (accepts "true", "1", "yes"). Default: False.
        error_exchange (str | None): Name of the RabbitMQ exchange for the error queue.
            Required if `mq_has_error_queue` is True, otherwise optional. Loaded from
            `MQ_ERROR_EXCHANGE`. Default: None.
        error_queue (str | None): Name of the RabbitMQ queue for failed messages. Required
            if `mq_has_error_queue` is True, otherwise optional. Loaded from `MQ_ERROR_QUEUE`.
            Default: None.
        error_routing_key (str | None): Routing key for the error queue. Required if
            `mq_has_error_queue` is True, otherwise optional. Loaded from `MQ_ERROR_ROUTING_KEY`.
            Default: None.

    Example:
        Create a configuration with an error queue:

        .. code-block:: python

            import os
            os.environ["MQ_HOST"] = "rabbitmq"
            os.environ["MQ_USER"] = "guest"
            os.environ["MQ_PASSWORD"] = "guest"
            os.environ["MQ_APPLICATION"] = "my_app"
            os.environ["MQ_PUBLISHER_EXCHANGE"] = "pub_ex"
            os.environ["MQ_PUBLISHER_QUEUE"] = "pub_queue"
            os.environ["MQ_PUBLISHER_ROUTING_KEY"] = "pub_key"
            os.environ["MQ_SUBSCRIBER_EXCHANGE"] = "sub_ex"
            os.environ["MQ_SUBSCRIBER_QUEUE"] = "sub_queue"
            os.environ["MQ_SUBSCRIBER_ROUTING_KEY"] = "sub_key"
            os.environ["MQ_HAS_ERROR_QUEUE"] = "true"
            os.environ["MQ_ERROR_EXCHANGE"] = "err_ex"
            os.environ["MQ_ERROR_QUEUE"] = "err_queue"
            os.environ["MQ_ERROR_ROUTING_KEY"] = "err_key"

            config = MQPipelineConfig.from_env_keys()
            print(config)  # Shows all fields, including error_exchange='err_ex'

        Without error queue:

        .. code-block:: python

            os.environ["MQ_HAS_ERROR_QUEUE"] = "false"
            config = MQPipelineConfig.from_env_keys()
            print(config.error_exchange)  # Outputs: None
    """

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

    class Config:  # pylint: disable=too-few-public-methods
        """Pydantic configuration settings for MQPipelineConfig.

        Configures how Pydantic processes environment variables and `.env` files.

        Attributes:
            env_file_encoding (str): Encoding for `.env` files, set to "utf-8" for
                consistent reading of environment variables.
        """
        env_file_encoding = "utf-8"

    @classmethod
    def from_env_keys(cls, env_keys: Mapping[str, str] = None):
        """Create a configuration instance from environment variables with custom key mappings.

        This method allows overriding default environment variable names by providing a
        dictionary of custom keys. It ensures required fields are present and handles
        conditional logic for error queue fields based on `mq_has_error_queue`.

        Args:
            env_keys (Mapping[str, str], optional): A dictionary mapping configuration field
                names to custom environment variable names. If not provided, default
                environment variable names (e.g., `MQ_HOST`, `MQ_PUBLISHER_EXCHANGE`) are used.

        Returns:
            MQPipelineConfig: An instance of the configuration with values loaded from
                environment variables.

        Raises:
            RuntimeError: If a required environment variable is missing (e.g., `MQ_HOST` or
                `MQ_ERROR_EXCHANGE` when `mq_has_error_queue` is True).

        Example:
            Use custom environment variable names:

            .. code-block:: python

                custom_keys = {
                    "publisher_exchange": "CUSTOM_PUB_EXCHANGE",
                    "publisher_queue": "CUSTOM_PUB_QUEUE",
                    "publisher_routing_key": "CUSTOM_PUB_KEY",
                    "subscriber_exchange": "CUSTOM_SUB_EXCHANGE",
                    "subscriber_queue": "CUSTOM_SUB_QUEUE",
                    "subscriber_routing_key": "CUSTOM_SUB_KEY",
                    "error_exchange": "CUSTOM_ERR_EXCHANGE",
                    "error_queue": "CUSTOM_ERR_QUEUE",
                    "error_routing_key": "CUSTOM_ERR_KEY",
                    "mq_has_error_queue": "CUSTOM_HAS_ERROR_QUEUE",
                }
                os.environ["CUSTOM_PUB_EXCHANGE"] = "my_exchange"
                os.environ["CUSTOM_HAS_ERROR_QUEUE"] = "true"
                os.environ["CUSTOM_ERR_EXCHANGE"] = "error_ex"
                config = MQPipelineConfig.from_env_keys(custom_keys)
                print(config.publisher_exchange)  # Outputs: my_exchange
                print(config.mq_has_error_queue)  # Outputs: True
        """
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
        """Retrieve a required environment variable, raising an error if missing.

        This helper method checks if an environment variable exists and returns its value.
        If the variable is not set or empty, it raises a `RuntimeError`.

        Args:
            key (str): The name of the environment variable to retrieve (e.g., `MQ_HOST`).

        Returns:
            str: The value of the environment variable.

        Raises:
            RuntimeError: If the environment variable is not set or empty.

        Example:
            .. code-block:: python

                os.environ["MQ_HOST"] = "rabbitmq"
                value = MQPipelineConfig._get_required_env("MQ_HOST")
                print(value)  # Outputs: rabbitmq

                # This will raise RuntimeError:
                MQPipelineConfig._get_required_env("MISSING_VAR")
        """
        value = os.getenv(key)
        if not value:
            raise RuntimeError(f"Missing required environment variable: {key}")
        return value

    @computed_field
    @property
    def mq_client_hostname(self) -> str:
        """Get the local server hostname, normalized to lowercase without domain.

        Retrieves the hostname of the machine running the application. On Windows, it uses
        the `COMPUTERNAME` environment variable; on other platforms (e.g., Linux, macOS), it
        uses `HOSTNAME`. If the variable is not set, defaults to "unknown". The hostname is
        converted to uppercase, the domain part (if any) is removed, and the result is
        converted to lowercase.

        Returns:
            str: The normalized hostname (e.g., "myhost" instead of "myhost.domain.com").

        Example:
            .. code-block:: python

                import os
                os.environ["HOSTNAME"] = "myhost.domain.com"
                config = MQPipelineConfig()
                print(config.mq_client_hostname)  # Outputs: myhost

                # On Windows with COMPUTERNAME
                os.environ["COMPUTERNAME"] = "MYPC"
                config = MQPipelineConfig()
                print(config.mq_client_hostname)  # Outputs: mypc
        """
        name = ('COMPUTERNAME' if sys.platform == 'win32' else 'HOSTNAME')
        return os.getenv(name, "unknown").upper().split('.')[0].lower()
