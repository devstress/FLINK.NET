<#
    This script reproduces the GitHub integration test workflow on a Windows
    machine. Invoke it with PowerShell using the following command:

        pwsh scripts/run-integration-tests-in-windows-os.ps1
#>

param(
    [string]$SimMessages = "1000000"
)

function Install-DotNet {
    if (-not (Get-Command dotnet -ErrorAction SilentlyContinue)) {
        Write-Host ".NET SDK not found. Installing via winget..."
        winget install --id Microsoft.DotNet.SDK.8 -e --accept-package-agreements --accept-source-agreements
    }
}

function Install-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Write-Host "Docker CLI not found. Installing Docker Desktop via winget..."
        winget install --id Docker.DockerDesktop -e --accept-package-agreements --accept-source-agreements
    }
}

Install-DotNet
Install-Docker

if (Get-Service -Name com.docker.service -ErrorAction SilentlyContinue) {
    Start-Service com.docker.service
    Write-Host "Waiting for Docker to start..."
    Start-Sleep -Seconds 15
    docker info
} else {
    Write-Warning "Docker service not found. Ensure Docker Desktop is running."
}

# Restore workloads
Write-Host "Restoring workloads..."
dotnet workload restore FlinkDotNet/FlinkDotNet.sln
dotnet workload restore FlinkDotNetAspire/FlinkDotNetAspire.sln
dotnet workload restore FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln

# Acquire Integration Test Docker image
if ($env:FLINK_IMAGE_REPOSITORY) {
    $imageName = "$($env:FLINK_IMAGE_REPOSITORY)/flink-dotnet-linux:latest"
} else {
    $imageName = "ghcr.io/devstress/flink-dotnet-linux:latest"
}
Write-Host "Pulling docker image $imageName..."
docker pull $imageName

# Start container
$containerName = "flink-dotnet-integration"
$env:SIMULATOR_NUM_MESSAGES = $SimMessages
docker run -d --name $containerName -e SIMULATOR_NUM_MESSAGES=$SimMessages -e ASPIRE_ALLOW_UNSECURED_TRANSPORT="true" -e ASPNETCORE_URLS="http://0.0.0.0:5199" -e DOTNET_DASHBOARD_OTLP_ENDPOINT_URL="http://localhost:4317" -p 5199:5199 -p 6379:6379 -p 9092:9092 -p 8088:8088 -p 50051:50051 -p 4317:4317 $imageName
Write-Host "Waiting for container to initialize..."
Start-Sleep -Seconds 30

$verifier = "./FlinkDotNetAspire/IntegrationTestVerifier/bin/Release/net8.0/FlinkDotNet.IntegrationTestVerifier.dll"
$maxAttempts = 10
$delaySeconds = 15
for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    Write-Host "Health check attempt $attempt/$maxAttempts..."
    dotnet $verifier --health-check
    if ($LASTEXITCODE -eq 0) { Write-Host "Health check PASSED."; break }
    Write-Host "Health check FAILED. Waiting $delaySeconds seconds before retry..."
    Start-Sleep -Seconds $delaySeconds
    if ($attempt -eq $maxAttempts) { Write-Host "Max attempts reached."; docker stop $containerName | Out-Null; docker rm $containerName | Out-Null; exit 1 }
}

Write-Host "Running verification tests..."
dotnet $verifier
$exitCode = $LASTEXITCODE

docker stop $containerName | Out-Null
docker rm $containerName | Out-Null
exit $exitCode

