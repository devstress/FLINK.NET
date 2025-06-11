#!/bin/bash
# build-all-sln.sh - Restore and build all major solutions in sequence.
# Usage: ./build-all-sln.sh
# 
# For advanced build with warning detection, use: ./local-build-analysis.ps1

set -e  # Exit on first error

# Navigate to repository root
cd "$(dirname "$0")"
ROOT="$(pwd)"

# Function to build a solution
build_solution() {
    local SLN="$1"
    
    if [ ! -f "$SLN" ]; then
        echo "Solution not found: $SLN"
        exit 1
    fi
    
    echo "=== Restoring $SLN ==="
    dotnet restore "$SLN"
    
    echo "=== Building $SLN ==="
    dotnet build "$SLN"
    
    echo ""
}

# Check if dotnet CLI is available
if ! command -v dotnet &> /dev/null; then
    echo ".NET SDK not found. Please install .NET 8.0 or later."
    exit 1
fi

# Build all solutions
build_solution "$ROOT/FlinkDotNet/FlinkDotNet.sln"
build_solution "$ROOT/FlinkDotNetAspire/FlinkDotNetAspire.sln"
build_solution "$ROOT/FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln"

echo "All solutions built successfully."