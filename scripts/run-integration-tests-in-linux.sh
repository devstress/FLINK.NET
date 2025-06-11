#!/usr/bin/env bash
# This script mirrors the GitHub integration test workflow on a Linux machine.
# Usage: ./scripts/run-integration-tests-in-linux.sh [SimMessages]
# Requires Docker and the .NET 8 SDK. Aspire uses Docker to start Redis and
# Kafka containers automatically.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

SIM_MESSAGES="${1:-1000000}"
APPHOST_PROJECT="FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost/FlinkDotNetAspire.AppHost.AppHost.csproj"
APPHOST_PID=""

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

start_apphost() {
    ASPIRE_ALLOW_UNSECURED_TRANSPORT="true" \
    SIMULATOR_NUM_MESSAGES="$SIM_MESSAGES" \
    dotnet run --no-build --configuration Release --project "$APPHOST_PROJECT" > apphost.log 2>&1 &
    APPHOST_PID=$!
}

stop_apphost() {
    if [[ -n "$APPHOST_PID" ]]; then
        kill "$APPHOST_PID" 2>/dev/null || true
        wait "$APPHOST_PID" 2>/dev/null || true
        echo "apphost.log contents:" && cat apphost.log
    fi
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

trap stop_apphost EXIT

check_dotnet
check_docker
build_verifier
start_apphost

echo "Waiting for AppHost to initialize..."
sleep 30

if ! health_check; then
    echo "Health check failed." >&2
    exit 1
fi

run_tests
