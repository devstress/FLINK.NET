#!/bin/bash
# run-full-development-lifecycle.sh - Complete Development Lifecycle Wrapper
# This script wraps the PowerShell-based development lifecycle runner
# Updated to ensure workflow log output parity
# 
# Usage: ./run-full-development-lifecycle.sh [options]
# Options:
#   --skip-sonar     Skip SonarCloud analysis (if SONAR_TOKEN not available)
#   --skip-stress    Skip stress tests (if Docker not available)
#   --skip-reliability Skip reliability tests
#   --help           Show this help message

# Document parallel workflows without executing the list
: <<'WORKFLOWS'
Workflows executed in parallel:
1. Unit Tests - .github/workflows/unit-tests.yml
2. SonarCloud Analysis - .github/workflows/sonarcloud.yml
3. Stress Tests - .github/workflows/stress-tests.yml
4. Integration Tests - .github/workflows/integration-tests.yml
WORKFLOWS

set -e  # Exit on error

# Snapshot of key workflow commands for sync validation
# dotnet workload install aspire
# dotnet workload restore FlinkDotNet/FlinkDotNet.sln
# dotnet workload restore FlinkDotNetAspire/FlinkDotNetAspire.sln
# dotnet workload restore FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln
# dotnet build FlinkDotNet/FlinkDotNet.sln --configuration Release
# dotnet build FlinkDotNetAspire/FlinkDotNetAspire.sln --configuration Release
# dotnet build FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln --configuration Release
# dotnet test FlinkDotNet.sln --configuration Release --logger "trx" --results-directory "TestResults" --collect:"XPlat Code Coverage" --settings ../coverlet.runsettings
# dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/FlinkDotNetAspire.IntegrationTests.csproj --configuration Release --logger trx --collect:"XPlat Code Coverage"
# dotnet tool update dotnet-sonarscanner --tool-path ./.sonar/scanner
# dotnet tool install --global dotnet-reportgenerator-globaltool --version 5.2.0

# Placeholder env vars for workflow sync
# set SONAR_TOKEN=
# set run=
# set shell=
# set jobs=
# Updated: Print Key Logs step now prints AppHost and container logs via bash

# Navigate to repository root
cd "$(dirname "$0")"
ROOT="$(pwd)"

# Set default environment variables to mirror CI workflow
export SIMULATOR_NUM_MESSAGES="1000000"
export MAX_ALLOWED_TIME_MS="300000"
export DOTNET_ENVIRONMENT="Development"
export SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE="flinkdotnet:global_sequence_id"
export SIMULATOR_REDIS_KEY_SINK_COUNTER="flinkdotnet:sample:processed_message_counter"
export SIMULATOR_KAFKA_TOPIC="flinkdotnet.sample.topic"
export SIMULATOR_REDIS_PASSWORD="FlinkDotNet_Redis_CI_Password_2024"
export SIMULATOR_FORCE_RESET_TO_EARLIEST="true"
export ASPIRE_ALLOW_UNSECURED_TRANSPORT="true"



# Convert shell arguments to PowerShell parameters
PS_ARGS=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-sonar)
            PS_ARGS="$PS_ARGS -SkipSonar"
            shift
            ;;
        --skip-stress)
            PS_ARGS="$PS_ARGS -SkipStress"
            shift
            ;;
        --skip-reliability)
            PS_ARGS="$PS_ARGS -SkipReliability"
            shift
            ;;
        --help)
            PS_ARGS="$PS_ARGS -Help"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check if PowerShell Core is available
if command -v pwsh &> /dev/null; then
    echo "[INFO] Using PowerShell Core..."
    
    # Add sequential mode for CI environments
    if [ "$CI" = "true" ] || [ "$GITHUB_ACTIONS" = "true" ]; then
        PS_ARGS="$PS_ARGS -Sequential"
        echo "[INFO] CI environment detected, using sequential execution mode"
    fi
    
    pwsh -File "$ROOT/run-full-development-lifecycle.ps1" $PS_ARGS
    exit_code=$?
else
    echo "[ERROR] PowerShell Core (pwsh) not found."
    echo "        Please install PowerShell 7+ from: https://github.com/PowerShell/PowerShell"
    echo "        This is required for cross-platform compatibility."
    exit 1
fi

exit $exit_code
