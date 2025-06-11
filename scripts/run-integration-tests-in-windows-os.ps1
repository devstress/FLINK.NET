<#
    This script reproduces the GitHub integration test workflow on a Windows
    machine. Invoke it with PowerShell using the following command:

        pwsh scripts/run-integration-tests-in-windows-os.ps1
#>

<#
    Note: This script must be executed from an elevated PowerShell session.
    The commands it runs require administrative privileges to start Docker
    services and pull container images.
#>

param(
    [string]$SimMessages = "1000000"
)

# Ensure script executes from its own directory so relative paths resolve.
Set-Location -Path $PSScriptRoot

function Ensure-Admin {
    $current = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($current)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        Write-Error "Administrator privileges are required. Please re-run this script in an elevated PowerShell session."
        exit 1
    }
}

function Install-DotNet {
    $dotnet = Get-Command dotnet -ErrorAction SilentlyContinue
    if (-not $dotnet) {
        Write-Host ".NET SDK not found. Installing via winget..."
        winget install --id Microsoft.DotNet.SDK.8 -e --accept-package-agreements --accept-source-agreements
        return
    }

    try {
        $version = [version](dotnet --version)
        if ($version.Major -ge 8) {
            Write-Host ".NET SDK version $version detected. Installation skipped."
        } else {
            Write-Host ".NET SDK version $version detected. Installing via winget..."
            winget install --id Microsoft.DotNet.SDK.8 -e --accept-package-agreements --accept-source-agreements
        }
    } catch {
        Write-Warning "Unable to determine .NET version. Proceeding without installation."
    }
}

function Check-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Write-Error "Docker CLI not found. Please install Docker Desktop and ensure it is running."
        exit 1
    }
}

function Build-Verifier {
    dotnet build ../FlinkDotNetAspire/IntegrationTestVerifier/IntegrationTestVerifier.csproj -c Release
}

Ensure-Admin
Install-DotNet
Check-Docker

if (Get-Service -Name com.docker.service -ErrorAction SilentlyContinue) {
    try {
        Start-Service com.docker.service -ErrorAction Stop
    } catch {
        Write-Warning "Failed to start Docker service. Try running PowerShell as Administrator or start Docker Desktop manually. $_"
    }
    Write-Host "Waiting for Docker to start..."
    Start-Sleep -Seconds 15
    docker info
} else {
    Write-Warning "Docker service not found. Ensure Docker Desktop is installed and running."
}

# Restore workloads
Write-Host "Restoring workloads..."
dotnet workload restore ../FlinkDotNet/FlinkDotNet.sln
dotnet workload restore ../FlinkDotNetAspire/FlinkDotNetAspire.sln
dotnet workload restore ../FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln

Build-Verifier


# Start the Aspire AppHost locally
$env:SIMULATOR_NUM_MESSAGES = $SimMessages
$env:ASPIRE_ALLOW_UNSECURED_TRANSPORT = "true"
$appHostProject = "../FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost/FlinkDotNetAspire.AppHost.AppHost.csproj"
$appHost = Start-Process "dotnet" "run --no-build --configuration Release --project $appHostProject" -RedirectStandardOutput apphost.out.log -RedirectStandardError apphost.err.log -PassThru
Write-Host "Waiting for AppHost to initialize..."
Start-Sleep -Seconds 30

$verifier = "../FlinkDotNetAspire/IntegrationTestVerifier/bin/Release/net8.0/FlinkDotNet.IntegrationTestVerifier.dll"
$maxAttempts = 10
$delaySeconds = 15
for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    Write-Host "Health check attempt $attempt/$maxAttempts..."
    dotnet $verifier --health-check
    if ($LASTEXITCODE -eq 0) { Write-Host "Health check PASSED."; break }
    Write-Host "Health check FAILED. Waiting $delaySeconds seconds before retry..."
    Start-Sleep -Seconds $delaySeconds
    if ($attempt -eq $maxAttempts) { Write-Host "Max attempts reached."; if (-not $appHost.HasExited) { Stop-Process -Id $appHost.Id -Force -ErrorAction SilentlyContinue }; exit 1 }
}

Write-Host "Running verification tests..."
dotnet $verifier
$exitCode = $LASTEXITCODE

$null = Stop-Process -Id $appHost.Id -Force -ErrorAction SilentlyContinue
Write-Host "apphost.out.log contents:"; Get-Content apphost.out.log
exit $exitCode

