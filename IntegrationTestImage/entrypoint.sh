#!/bin/sh
set -e

# Start Docker daemon
/dockerd-entrypoint.sh &

# Wait for Docker to be ready
until docker info >/dev/null 2>&1; do
  sleep 1
done

# Load cached images or pull if missing
if [ -f /redis.tar ]; then
  docker load -i /redis.tar || true
else
  docker pull redis:latest
fi

if [ -f /kafka.tar ]; then
  docker load -i /kafka.tar || true
else
  docker pull confluentinc/cp-kafka:latest
fi

exec ./IntegrationTestImage "$@"
