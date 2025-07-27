# MQPipeline

MQPipeline is a Python library for processing messages from RabbitMQ queues. It simplifies consuming, processing, and publishing messages with a custom handler, supporting durable queues and optional error handling. Configuration is managed via environment variables using Pydantic.

## Features

- Connects to RabbitMQ with configurable exchanges, queues, and routing keys.
- Processes messages asynchronously with a user-defined handler.
- Supports an optional error queue for failed messages.
- Ensures graceful shutdown on SIGTERM/SIGINT.
- Includes a sample app and test setup with Podman Compose.

## Installation

### Prerequisites

- Python 3.12+
- RabbitMQ server (e.g., via Docker)
- Podman and Podman Compose (for testing)
- Tox

### Setup

Clone the repository and install dependencies:

```bash
git clone <repository-url>
cd mqpipeline
tox -e dist
```

It produces a wheel file in the `dist/` directory.

## Usage

Configure Environment Variables

Set up a .env file in tests/ or export variables:
```bash
export MQ_HOST=rabbitmq
export MQ_USER=guest
export MQ_PASSWORD=guest
export MQ_VHOST= # empty for default vhost /
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
```
If **MQ_HAS_ERROR_QUEUE** is set to true, the error queue will be used for failed messages. Otherwise MQ_ERROR_EXCHANGE, MQ_ERROR_QUEUE, and MQ_ERROR_ROUTING_KEY are ignored.

### Sample Application

Please refer to the sample app in `tests/sample_app/app.py` for an example of how to use the library. It demonstrates how to set up a message handler, configure RabbitMQ connections, and start consuming messages.

##Testing

For local testing, run:

```bash
./test.sh
```

This runs linting (pylint), security checks (bandit), builds the wheel, and starts the sample app with Podman Compose.

### Send Test Messages

Use the RabbitMQ UI (http://localhost:15672) or curl:
```bash
curl -X POST http://guest:guest@localhost:15672/api/exchanges/%2F/sub_ex/publish \
    -H "Content-Type: application/json" \
    -d '{
        "routing_key": "sub_key",
        "payload": "{\"sleep_time\": 2, \"produce_error\": false}",
        "payload_encoding": "string",
        "properties": {"delivery_mode": 2}
    }'
```

## Documentation

To build documentation locally for ReadTheDocs:

```bash
tox -e docs
```

View the output in `docs/_build/html/index.html`. The documentation covers MQPipelineConfig, MQPipeline and Sample Application.

## Contributing

Submit pull requests or issues to the repository.

## License

MIT License. See LICENSE for details.
