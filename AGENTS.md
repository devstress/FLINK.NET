This repo uses .NET 8.0 and the .NET Aspire workload.
Install Docker CE so Aspire's AppHost can start Redis and Kafka containers.
Typical commands:
- `dotnet test FlinkDotNet/FlinkDotNet.sln -v minimal` for unit tests.
- Integration tests mimic `.github/workflows/integration-tests.yml` and need Docker running.

