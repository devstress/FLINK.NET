#!/bin/bash
# run-full-development-lifecycle.sh - Run all GitHub workflows locally in parallel
# This script mirrors the GitHub Actions workflows to run locally for development validation
# 
# Workflows executed in parallel:
# 1. Unit Tests          - Runs .NET unit tests with coverage collection
# 2. SonarCloud Analysis - Builds solutions and performs code analysis  
# 3. Stress Tests        - Runs Aspire stress tests with Redis/Kafka
# 4. Integration Tests   - Runs Aspire integration tests
#
# Usage: ./run-full-development-lifecycle.sh [options]
# Options:
#   --skip-sonar     Skip SonarCloud analysis (if SONAR_TOKEN not available)
#   --skip-stress    Skip stress tests (if Docker not available)
#   --help           Show this help message

set -e  # Exit on error

# Check if running with appropriate privileges (sudo or root)
check_admin_privileges() {
    if [[ $EUID -ne 0 ]] && ! sudo -n true 2>/dev/null; then
        echo "❌ This script requires administrator privileges."
        echo "   Please run with 'sudo' or as root user:"
        echo "   sudo ./run-full-development-lifecycle.sh"
        echo ""
        echo "   Administrator privileges are required for:"
        echo "   - Installing missing prerequisites (.NET, Java, Docker, PowerShell)"
        echo "   - Docker container operations"
        echo "   - System-wide tool installations"
        exit 1
    fi
    echo "✅ Administrator privileges confirmed"
}

# Check admin privileges early
check_admin_privileges

# Initialize variables
SKIP_SONAR=0
SKIP_STRESS=0
SHOW_HELP=0

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-sonar)
            SKIP_SONAR=1
            shift
            ;;
        --skip-stress)
            SKIP_STRESS=1
            shift
            ;;
        --help)
            SHOW_HELP=1
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

if [[ $SHOW_HELP -eq 1 ]]; then
    cat << 'EOF'

Local GitHub Workflows Runner

This script runs all GitHub Actions workflows locally in parallel:
  1. Unit Tests          - .NET unit tests with coverage
  2. SonarCloud Analysis - Code analysis and build validation
  3. Stress Tests        - Aspire stress tests with containers
  4. Integration Tests   - Aspire integration tests

Usage: ./run-full-development-lifecycle.sh [options]

Options:
  --skip-sonar     Skip SonarCloud analysis (useful if no SONAR_TOKEN)
  --skip-stress    Skip stress tests (useful if Docker unavailable)
  --help           Show this help message

Prerequisites:
  - .NET 8.0 SDK
  - Java 17+ (for SonarCloud)
  - Docker (for stress/integration tests)
  - PowerShell Core (pwsh)

Environment Variables:
  SONAR_TOKEN              SonarCloud authentication token
  SIMULATOR_NUM_MESSAGES   Number of messages for stress tests (default: 1000000)

EOF
    exit 0
fi

# Navigate to repository root (since script is now in root)
cd "$(dirname "$0")"
ROOT="$(pwd)"

echo "================================================================"
echo "   Local GitHub Workflows Runner - Parallel Execution"
echo "================================================================"
echo "Repository: $ROOT"
echo "Timestamp: $(date)"
echo ""

# Helper functions
check_command() {
    local cmd="$1"
    local name="$2"
    local url="$3"
    
    if command -v "$cmd" &> /dev/null; then
        local version
        case "$cmd" in
            dotnet)
                version=$(dotnet --version 2>/dev/null || echo "unknown")
                ;;
            java)
                version=$(java -version 2>&1 | head -1 | awk -F '"' '{print $2}' || echo "unknown")
                ;;
            docker)
                version=$(docker --version 2>/dev/null | awk '{print $3}' | sed 's/,//' || echo "unknown")
                ;;
            pwsh)
                version=$(pwsh -Command '$PSVersionTable.PSVersion.ToString()' 2>/dev/null || echo "unknown")
                ;;
            *)
                version="found"
                ;;
        esac
        echo "✅ $name: $version"
        return 0
    else
        echo "❌ $name not found."
        if [[ -n "$url" ]]; then
            echo "   Please install from: $url"
        fi
        return 1
    fi
}

check_docker_running() {
    if docker info &> /dev/null; then
        echo "✅ Docker is running"
        return 0
    else
        echo "❌ Docker is not running or not accessible"
        
        # Try to start Docker Desktop if it exists
        if command -v "Docker Desktop" &> /dev/null; then
            echo "   Attempting to start Docker Desktop..."
            open -a "Docker Desktop" 2>/dev/null || true
            
            # Wait for Docker to start (max 60 seconds)
            local wait_count=0
            while [[ $wait_count -lt 60 ]]; do
                sleep 5
                wait_count=$((wait_count + 5))
                if docker info &> /dev/null; then
                    echo "✅ Docker Desktop started successfully"
                    return 0
                fi
            done
            echo "❌ Docker Desktop failed to start automatically"
        fi
        
        echo "   Please start Docker Desktop manually before running this script"
        return 1
    fi
}

# Check prerequisites
echo "=== Checking Prerequisites ==="

# Parallel prerequisite checking for improved speed
check_prerequisites_parallel() {
    local dotnet_result=""
    local java_result=""
    local docker_result=""
    local pwsh_result=""
    
    # Create temporary files for parallel results
    local temp_dir="/tmp/prereq_check_$$"
    mkdir -p "$temp_dir"
    
    # Start parallel checks
    echo "Running prerequisite checks in parallel..."
    
    # Check .NET SDK
    (
        if check_command dotnet ".NET SDK" "https://dotnet.microsoft.com/download" >/dev/null 2>&1; then
            echo "success" > "$temp_dir/dotnet"
        else
            echo "fail" > "$temp_dir/dotnet"
        fi
    ) &
    local dotnet_pid=$!
    
    # Check Java (if needed)
    if [[ $SKIP_SONAR -eq 0 ]]; then
        (
            if check_command java "Java" "https://adoptopenjdk.net/" >/dev/null 2>&1; then
                echo "success" > "$temp_dir/java"
            else
                echo "fail" > "$temp_dir/java"
            fi
        ) &
        local java_pid=$!
    fi
    
    # Check Docker (if needed)
    if [[ $SKIP_STRESS -eq 0 ]]; then
        (
            if check_command docker "Docker" "https://docker.com/" >/dev/null 2>&1; then
                if check_docker_running >/dev/null 2>&1; then
                    echo "success" > "$temp_dir/docker"
                else
                    echo "fail_start" > "$temp_dir/docker"
                fi
            else
                echo "fail_install" > "$temp_dir/docker"
            fi
        ) &
        local docker_pid=$!
    fi
    
    # Check PowerShell Core
    (
        if check_command pwsh "PowerShell Core" "https://github.com/PowerShell/PowerShell" >/dev/null 2>&1; then
            echo "success" > "$temp_dir/pwsh"
        else
            echo "fail" > "$temp_dir/pwsh"
        fi
    ) &
    local pwsh_pid=$!
    
    # Wait for all parallel checks to complete
    wait $dotnet_pid
    [[ $SKIP_SONAR -eq 0 ]] && wait $java_pid
    [[ $SKIP_STRESS -eq 0 ]] && wait $docker_pid
    wait $pwsh_pid
    
    # Now display results in proper order and handle failures
    check_command dotnet ".NET SDK" "https://dotnet.microsoft.com/download"
    if [[ $? -ne 0 ]]; then
        rm -rf "$temp_dir"
        exit 1
    fi
    
    if [[ $SKIP_SONAR -eq 0 ]]; then
        if ! check_command java "Java" "https://adoptopenjdk.net/"; then
            echo "WARNING: Java not found. SonarCloud analysis will be skipped."
            SKIP_SONAR=1
        fi
    fi
    
    if [[ $SKIP_STRESS -eq 0 ]]; then
        local docker_result=$(cat "$temp_dir/docker" 2>/dev/null || echo "fail")
        if [[ "$docker_result" == "success" ]]; then
            check_command docker "Docker" "https://docker.com/"
            check_docker_running
        elif [[ "$docker_result" == "fail_start" ]]; then
            check_command docker "Docker" "https://docker.com/"
            if ! check_docker_running; then
                echo "WARNING: Docker available but not running. Stress tests will be skipped."
                SKIP_STRESS=1
            fi
        else
            if ! check_command docker "Docker" "https://docker.com/"; then
                echo "WARNING: Docker not available. Stress tests will be skipped."
                SKIP_STRESS=1
            fi
        fi
    fi
    
    check_command pwsh "PowerShell Core" "https://github.com/PowerShell/PowerShell"
    if [[ $? -ne 0 ]]; then
        echo "WARNING: PowerShell Core not found. Please install PowerShell 7+ manually:"
        echo "   https://github.com/PowerShell/PowerShell/releases"
        echo "   Some workflows may not function correctly without PowerShell Core."
        echo ""
        echo "Continuing without PowerShell Core..."
    fi
    
    # Clean up
    rm -rf "$temp_dir"
}

check_prerequisites_parallel

echo "Prerequisites check completed."
echo ""

# Create output directories for parallel execution in parallel with other setup
mkdir -p "$ROOT/workflow-logs" &
mkdir -p "$ROOT/scripts" &

# Set environment variables
export SIMULATOR_NUM_MESSAGES=${SIMULATOR_NUM_MESSAGES:-1000000}
export MAX_ALLOWED_TIME_MS=${MAX_ALLOWED_TIME_MS:-60000}
export ASPIRE_ALLOW_UNSECURED_TRANSPORT=true

# Wait for directory creation to complete
wait

echo "=== Starting Workflows in Parallel ==="
echo "Unit Tests: workflow-logs/unit-tests.log"
echo "SonarCloud: workflow-logs/sonarcloud.log"
if [[ $SKIP_STRESS -eq 0 ]]; then
    echo "Stress Tests: workflow-logs/stress-tests.log"
else
    echo "Stress Tests: SKIPPED (--skip-stress or Docker unavailable)"
fi
echo "Integration Tests: workflow-logs/integration-tests.log"
echo ""

# Create individual workflow scripts to run

# Start workflows in parallel using existing local scripts
declare -a PIDS=()
declare -a WORKFLOW_NAMES=()

# Start Unit Tests
echo "Starting Unit Tests workflow..."
pwsh -File "$ROOT/scripts/run-local-unit-tests.ps1" > "$ROOT/workflow-logs/unit-tests.log" 2>&1 &
PIDS+=($!)
WORKFLOW_NAMES+=("Unit Tests")

# Start SonarCloud (if not skipped)
if [[ $SKIP_SONAR -eq 0 ]]; then
    echo "Starting SonarCloud workflow..."
    pwsh -File "$ROOT/scripts/run-local-sonarcloud.ps1" > "$ROOT/workflow-logs/sonarcloud.log" 2>&1 &
    PIDS+=($!)
    WORKFLOW_NAMES+=("SonarCloud")
fi

# Start Stress Tests (if not skipped)
if [[ $SKIP_STRESS -eq 0 ]]; then
    echo "Starting Stress Tests workflow..."
    pwsh -File "$ROOT/scripts/run-local-stress-tests.ps1" > "$ROOT/workflow-logs/stress-tests.log" 2>&1 &
    PIDS+=($!)
    WORKFLOW_NAMES+=("Stress Tests")
fi

# Start Integration Tests
echo "Starting Integration Tests workflow..."
pwsh -File "$ROOT/scripts/run-integration-tests-in-linux.sh" > "$ROOT/workflow-logs/integration-tests.log" 2>&1 &
PIDS+=($!)
WORKFLOW_NAMES+=("Integration Tests")

echo ""
echo "All workflows started in parallel. Monitoring progress..."
echo "Use 'tail -f workflow-logs/*.log' to follow progress in real-time."
echo ""

# Monitor workflow progress
FAILED_WORKFLOWS=()
COMPLETED_WORKFLOWS=()

while [[ ${#PIDS[@]} -gt 0 ]]; do
    for i in "${!PIDS[@]}"; do
        pid="${PIDS[$i]}"
        name="${WORKFLOW_NAMES[$i]}"
        
        if ! kill -0 "$pid" 2>/dev/null; then
            # Process finished
            wait "$pid"
            exit_code=$?
            
            if [[ $exit_code -eq 0 ]]; then
                echo "[COMPLETED] $name workflow finished successfully"
                COMPLETED_WORKFLOWS+=("$name")
            else
                echo "[FAILED] $name workflow failed with exit code $exit_code"
                FAILED_WORKFLOWS+=("$name")
            fi
            
            # Remove from arrays
            unset PIDS[$i]
            unset WORKFLOW_NAMES[$i]
            PIDS=("${PIDS[@]}")
            WORKFLOW_NAMES=("${WORKFLOW_NAMES[@]}")
            break
        fi
    done
    
    if [[ ${#PIDS[@]} -gt 0 ]]; then
        sleep 1  # Reduced from 2 seconds to 1 second for faster monitoring
    fi
done

# All workflows completed, no cleanup needed for local scripts

# Report final results
echo ""
echo "=== Workflow Execution Summary ==="
echo "Completed: ${#COMPLETED_WORKFLOWS[@]}"
echo "Failed: ${#FAILED_WORKFLOWS[@]}"

if [[ ${#FAILED_WORKFLOWS[@]} -gt 0 ]]; then
    echo "Failed workflows: ${FAILED_WORKFLOWS[*]}"
    echo ""
    echo "❌ One or more workflows failed."
    echo "Check workflow-logs/ directory for error details:"
    for workflow in "${FAILED_WORKFLOWS[@]}"; do
        case "$workflow" in
            "Unit Tests")
                echo "  - workflow-logs/unit-tests.log"
                ;;
            "SonarCloud")
                echo "  - workflow-logs/sonarcloud.log"
                ;;
            "Stress Tests")
                echo "  - workflow-logs/stress-tests.log"
                ;;
            "Integration Tests")
                echo "  - workflow-logs/integration-tests.log"
                ;;
        esac
    done
    exit 1
else
    echo ""
    echo "✅ All workflows completed successfully!"
    echo "Check workflow-logs/ directory for detailed output."
    exit 0
fi