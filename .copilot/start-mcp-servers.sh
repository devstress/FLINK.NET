#!/bin/bash
# Start MCP servers for Flink.NET AI assistance

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MCP_DIR="${PROJECT_ROOT}/.copilot/mcp-servers"

echo "Starting Flink.NET MCP Servers..."
echo "Project Root: ${PROJECT_ROOT}"

# Set environment variables
export PROJECT_ROOT="${PROJECT_ROOT}"
export DOCS_PATH="${PROJECT_ROOT}/docs/wiki"
export SOLUTION_PATHS="FlinkDotNet/FlinkDotNet.sln;FlinkDotNetAspire/FlinkDotNetAspire.sln;FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln"
export TEST_COMMANDS="dotnet test FlinkDotNet/FlinkDotNet.sln -v minimal"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Python3 is required but not installed. Please install Python3."
    exit 1
fi

# Make server scripts executable
chmod +x "${MCP_DIR}"/*.py

echo "Starting Documentation Server..."
python3 "${MCP_DIR}/docs-server.py" &
DOCS_PID=$!

echo "Starting Codebase Analysis Server..."
python3 "${MCP_DIR}/codebase-server.py" &
CODEBASE_PID=$!

echo "Starting Testing Guidance Server..."
python3 "${MCP_DIR}/testing-server.py" &
TESTING_PID=$!

echo ""
echo "MCP Servers started successfully!"
echo "Documentation Server PID: ${DOCS_PID}"
echo "Codebase Analysis Server PID: ${CODEBASE_PID}" 
echo "Testing Guidance Server PID: ${TESTING_PID}"
echo ""
echo "Servers are now providing context to AI agents."
echo "Press Ctrl+C to stop all servers."

# Function to clean up background processes
cleanup() {
    echo "Stopping MCP servers..."
    kill $DOCS_PID $CODEBASE_PID $TESTING_PID 2>/dev/null || true
    echo "MCP servers stopped."
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Wait for all background processes
wait