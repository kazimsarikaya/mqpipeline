#!/bin/sh

echo "[wait-for-rabbit] Waiting for RabbitMQ..."

RETRIES=30
until nc -z rabbitmq 5672 || [ "$RETRIES" -eq 0 ]; do
  echo "[wait-for-rabbit] Waiting for rabbitmq:5672..."
  sleep 2
  RETRIES=$((RETRIES - 1))
done

if [ "$RETRIES" -eq 0 ]; then
  echo "[wait-for-rabbit] RabbitMQ not available, exiting."
  exit 1
fi

echo "[wait-for-rabbit] RabbitMQ is up. Starting app..."
exec /app/venv/bin/python app.py
