FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN python -m venv venv && \
    . venv/bin/activate && \
    pip install --no-cache-dir deps/mqpipeline-0.1.0-py3-none-any.whl

CMD ["/app/venv/bin/python", "app.py"]
