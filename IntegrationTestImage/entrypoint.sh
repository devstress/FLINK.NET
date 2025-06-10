#!/bin/sh
set -e

# If a host Docker socket is mounted, reuse it; otherwise start our own daemon
if [ -S /var/run/docker.sock ]; then
  echo "Using host Docker daemon"
else
  echo "Starting nested Docker daemon"
  /dockerd-entrypoint.sh &
  until docker info >/dev/null 2>&1; do
    sleep 1
  done
fi

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
