#!/bin/sh
set -e

# Start Docker daemon
/dockerd-entrypoint.sh &

# Wait for Docker to be ready
until docker info >/dev/null 2>&1; do
  sleep 1
done

# Load cached images
if [ -f /redis.tar ]; then
  docker load -i /redis.tar || true
fi
if [ -f /kafka.tar ]; then
  docker load -i /kafka.tar || true
fi

exec dotnet IntegrationTestImage.dll "$@"
