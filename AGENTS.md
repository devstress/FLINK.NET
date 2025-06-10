This repo uses .NET 8.0 and the .NET Aspire workload.
Install Docker CE and ensure the daemon is running so Aspire's AppHost can start Redis and Kafka containers.
If the dotnet CLI is missing, install the .NET 8 SDK using Ubuntu packages with:
`apt-get update && apt-get install -y dotnet-sdk-8.0`
The dotnet-install script can fail on newer distributions.
Typical commands:
- `dotnet test FlinkDotNet/FlinkDotNet.sln -v minimal` for unit tests.
- Integration tests mimic `.github/workflows/integration-tests.yml` and require Docker. They run inside a Linux container.
  They pull `ghcr.io/devstress/flink-dotnet-linux:latest` by default.
  Use `scripts/run-integration-tests-in-linux.sh` on Linux or `scripts/run-integration-tests-in-windows-os.ps1` on Windows to reproduce the workflow locally.
