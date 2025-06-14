#!/bin/bash
# run-full-development-lifecycle.sh - Complete Development Lifecycle Wrapper
# This script wraps the PowerShell-based development lifecycle runner
# 
# Usage: ./run-full-development-lifecycle.sh [options]
# Options:
#   --skip-sonar     Skip SonarCloud analysis (if SONAR_TOKEN not available)
#   --skip-stress    Skip stress tests (if Docker not available)
#   --skip-reliability Skip reliability tests
#   --help           Show this help message

set -e  # Exit on error

# Navigate to repository root
cd "$(dirname "$0")"
ROOT="$(pwd)"

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
    pwsh -File "$ROOT/run-full-development-lifecycle.ps1" $PS_ARGS
    exit_code=$?
else
    echo "[ERROR] PowerShell Core (pwsh) not found."
    echo "        Please install PowerShell 7+ from: https://github.com/PowerShell/PowerShell"
    echo "        This is required for cross-platform compatibility."
    exit 1
fi

exit $exit_code