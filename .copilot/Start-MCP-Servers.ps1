# Start MCP servers for Flink.NET AI assistance (Windows)
param(
    [switch]$Help
)

if ($Help) {
    Write-Host "Start-MCP-Servers.ps1 - Start Model Context Protocol servers for Flink.NET AI assistance"
    Write-Host ""
    Write-Host "DESCRIPTION:"
    Write-Host "    Starts Python-based MCP servers that provide project context to AI agents."
    Write-Host "    The servers analyze documentation, codebase patterns, and testing approaches."
    Write-Host ""
    Write-Host "REQUIREMENTS:"
    Write-Host "    - Python 3.x installed and available in PATH"
    Write-Host "    - Run from project root directory or .copilot directory"
    Write-Host ""
    Write-Host "USAGE:"
    Write-Host "    .\\.copilot\\Start-MCP-Servers.ps1"
    Write-Host "    .\\.copilot\\Start-MCP-Servers.ps1 -Help"
    exit 0
}

# Get project root directory
$ProjectRoot = if ($PSScriptRoot -match "\\.copilot$") {
    Split-Path $PSScriptRoot -Parent
} else {
    $PSScriptRoot
}

$McpDir = Join-Path $ProjectRoot ".copilot\mcp-servers"

Write-Host "Starting Flink.NET MCP Servers..." -ForegroundColor Green
Write-Host "Project Root: $ProjectRoot"

# Set environment variables
$env:PROJECT_ROOT = $ProjectRoot
$env:DOCS_PATH = Join-Path $ProjectRoot "docs\wiki"
$env:SOLUTION_PATHS = "FlinkDotNet/FlinkDotNet.sln;FlinkDotNetAspire/FlinkDotNetAspire.sln;FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln"
$env:TEST_COMMANDS = "dotnet test FlinkDotNet/FlinkDotNet.sln -v minimal"

# Check if Python is available
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error "Python is required but not found in PATH. Please install Python 3.x and ensure it's available in PATH."
    exit 1
}

Write-Host "Starting Documentation Server..." -ForegroundColor Yellow
$docsJob = Start-Job -ScriptBlock {
    param($scriptPath, $projectRoot, $docsPath)
    $env:PROJECT_ROOT = $projectRoot
    $env:DOCS_PATH = $docsPath
    python $scriptPath
} -ArgumentList (Join-Path $McpDir "docs-server.py"), $ProjectRoot, $env:DOCS_PATH

Write-Host "Starting Codebase Analysis Server..." -ForegroundColor Yellow  
$codebaseJob = Start-Job -ScriptBlock {
    param($scriptPath, $projectRoot, $solutionPaths)
    $env:PROJECT_ROOT = $projectRoot
    $env:SOLUTION_PATHS = $solutionPaths
    python $scriptPath
} -ArgumentList (Join-Path $McpDir "codebase-server.py"), $ProjectRoot, $env:SOLUTION_PATHS

Write-Host "Starting Testing Guidance Server..." -ForegroundColor Yellow
$testingJob = Start-Job -ScriptBlock {
    param($scriptPath, $projectRoot, $testCommands)
    $env:PROJECT_ROOT = $projectRoot
    $env:TEST_COMMANDS = $testCommands
    python $scriptPath
} -ArgumentList (Join-Path $McpDir "testing-server.py"), $ProjectRoot, $env:TEST_COMMANDS

# Wait a moment for servers to start
Start-Sleep -Seconds 2

Write-Host ""
Write-Host "MCP Servers started successfully!" -ForegroundColor Green
Write-Host "Documentation Server Job ID: $($docsJob.Id)"
Write-Host "Codebase Analysis Server Job ID: $($codebaseJob.Id)"
Write-Host "Testing Guidance Server Job ID: $($testingJob.Id)"
Write-Host ""
Write-Host "Servers are now providing context to AI agents." -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop all servers or use Stop-Job to stop individual servers."
Write-Host ""
Write-Host "To monitor server output:"
Write-Host "    Receive-Job -Job $($docsJob.Id) -Keep"
Write-Host "    Receive-Job -Job $($codebaseJob.Id) -Keep" 
Write-Host "    Receive-Job -Job $($testingJob.Id) -Keep"

# Function to clean up when script is interrupted
$cleanup = {
    Write-Host "Stopping MCP servers..." -ForegroundColor Yellow
    Stop-Job $docsJob, $codebaseJob, $testingJob -ErrorAction SilentlyContinue
    Remove-Job $docsJob, $codebaseJob, $testingJob -Force -ErrorAction SilentlyContinue
    Write-Host "MCP servers stopped." -ForegroundColor Green
}

# Register cleanup for Ctrl+C
Register-EngineEvent PowerShell.Exiting -Action $cleanup | Out-Null

try {
    # Keep script running and monitor jobs
    while ($true) {
        $runningJobs = Get-Job | Where-Object { $_.State -eq "Running" }
        if ($runningJobs.Count -eq 0) {
            Write-Host "All MCP servers have stopped." -ForegroundColor Red
            break
        }
        Start-Sleep -Seconds 5
    }
}
finally {
    & $cleanup
}