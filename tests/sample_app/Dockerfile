FROM python:3.12-slim

# Install tini and netcat early for caching benefits
RUN apt-get update && \
    apt-get install -y --no-install-recommends tini netcat-openbsd && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set tini as the init process
ENTRYPOINT ["/usr/bin/tini", "--"]

WORKDIR /app

# Copy code and install dependencies
COPY . .

RUN python -m venv venv && \
    ./venv/bin/pip install --no-cache-dir deps/mqpipeline-0.1.0-py3-none-any.whl

CMD ["/app/venv/bin/python", "app.py"]

