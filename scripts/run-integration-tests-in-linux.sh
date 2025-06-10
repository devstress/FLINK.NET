#!/usr/bin/env bash
# This script mirrors the GitHub integration test workflow on a Linux machine.
# Usage: ./scripts/run-integration-tests-in-linux.sh [SimMessages]
# Requires Docker and the .NET 8 SDK.

set -euo pipefail

SIM_MESSAGES="${1:-1000000}"
IMAGE_REPO="${FLINK_IMAGE_REPOSITORY:-ghcr.io/devstress}"
IMAGE_NAME="${IMAGE_REPO}/flink-dotnet-linux:latest"
CONTAINER_NAME="flink-dotnet-integration"

check_dotnet() {
    if ! command -v dotnet >/dev/null 2>&1; then
        echo ".NET SDK not found. Installing via apt-get..."
        sudo apt-get update
        sudo apt-get install -y dotnet-sdk-8.0
    fi
}

check_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        echo "Docker CLI not found. Please install Docker CE and ensure it is running." >&2
        exit 1
    fi
    docker info >/dev/null
}

build_verifier() {
    dotnet build FlinkDotNetAspire/IntegrationTestVerifier/IntegrationTestVerifier.csproj -c Release
}

pull_image() {
    echo "Pulling Docker image $IMAGE_NAME..."
    docker pull "$IMAGE_NAME"
}

start_container() {
    docker run -d --name "$CONTAINER_NAME" \
        -e SIMULATOR_NUM_MESSAGES="$SIM_MESSAGES" \
        -e ASPIRE_ALLOW_UNSECURED_TRANSPORT="true" \
        -e ASPNETCORE_URLS="http://0.0.0.0:5199" \
        -e DOTNET_DASHBOARD_OTLP_ENDPOINT_URL="http://localhost:4317" \
        -p 5199:5199 -p 6379:6379 -p 9092:9092 \
        -p 8088:8088 -p 50051:50051 -p 4317:4317 \
        "$IMAGE_NAME"
}

stop_container() {
    docker stop "$CONTAINER_NAME" >/dev/null || true
    docker rm "$CONTAINER_NAME" >/dev/null || true
}

health_check() {
    local verifier="./FlinkDotNetAspire/IntegrationTestVerifier/bin/Release/net8.0/FlinkDotNet.IntegrationTestVerifier.dll"
    for attempt in {1..10}; do
        echo "Health check attempt $attempt/10..."
        if dotnet "$verifier" --health-check; then
            echo "Health check PASSED."
            return 0
        fi
        echo "Health check FAILED. Retrying in 15s..."
        sleep 15
    done
    return 1
}

run_tests() {
    local verifier="./FlinkDotNetAspire/IntegrationTestVerifier/bin/Release/net8.0/FlinkDotNet.IntegrationTestVerifier.dll"
    dotnet "$verifier"
}

trap stop_container EXIT

check_dotnet
check_docker
build_verifier
pull_image
start_container

echo "Waiting for container to initialize..."
sleep 30

if ! health_check; then
    echo "Health check failed." >&2
    exit 1
fi

run_tests
