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

# Build
Write-Host "Building solutions..."
Push-Location FlinkDotNetAspire
 dotnet build FlinkDotNetAspire.sln --configuration Release
Pop-Location

# Start Aspire AppHost
$env:SIMULATOR_NUM_MESSAGES = $SimMessages
$env:ASPIRE_ALLOW_UNSECURED_TRANSPORT = "true"
$env:ASPNETCORE_URLS = "http://localhost:5199"
$env:DOTNET_DASHBOARD_OTLP_ENDPOINT_URL = "http://localhost:4317"

Push-Location FlinkDotNetAspire/FlinkDotNetAspire.AppHost
 dotnet publish FlinkDotNetAspire.AppHost.csproj --configuration Release -p:IsPublishable=true -p:AspireManifestPublishOutputPath=../publish -t:GenerateAspireManifest
 Copy-Item ../publish/manifest.json ../aspire-manifest.json -Force
 $process = Start-Process -FilePath dotnet -ArgumentList "run --project FlinkDotNetAspire.AppHost.csproj --no-launch-profile --no-build -c Release" -PassThru -RedirectStandardOutput ../../aspire-apphost.log -RedirectStandardError ../../aspire-apphost.err.log
 $aspirePid = $process.Id
Pop-Location

Write-Host "Aspire AppHost started with PID $aspirePid"
Write-Host "Waiting for Aspire AppHost to initialize..."
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
    if ($attempt -eq $maxAttempts) { Write-Host "Max attempts reached."; Stop-Process -Id $aspirePid -Force; exit 1 }
}

Write-Host "Running verification tests..."
dotnet $verifier
$exitCode = $LASTEXITCODE

Stop-Process -Id $aspirePid -Force
exit $exitCode

