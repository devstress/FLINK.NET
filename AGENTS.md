# Flink.NET Development Guide for AI Agents

## Project Overview
Flink.NET is a .NET-native stream processing framework inspired by FlinkDotnet, providing stateful stream processing, exactly-once semantics, and fault tolerance within the .NET ecosystem.

**Current Status**: Alpha/foundational development stage focused on Phase 1 core functionality for exactly-once FIFO processing.

## Technology Stack & Prerequisites

### Required Software
- **.NET 8.0 SDK**: Primary development framework
- **Docker Desktop**: Required for Aspire to orchestrate Redis and Kafka containers
- **.NET Aspire workload**: For local development orchestration

### Installation Commands
```bash
# Install .NET 8 SDK on Ubuntu
apt-get update && apt-get install -y dotnet-sdk-8.0

# Install .NET Aspire workload
dotnet workload install aspire

# Verify installation
dotnet --version  # Should show 8.0.x
```

## Project Structure

### Core Solutions
1. **FlinkDotNet/FlinkDotNet.sln**: Main framework implementation
   - Core libraries and abstractions
   - JobManager and TaskManager services  
   - Connectors and operators
   - Unit tests for all components

2. **FlinkDotNetAspire/FlinkDotNetAspire.sln**: Local development and simulation
   - FlinkJobSimulator: Sample applications
   - IntegrationTestVerifier: End-to-end testing
   - Aspire AppHost: Service orchestration

3. **FlinkDotNet.WebUI/FlinkDotNet.WebUI.sln**: Blazor WebAssembly UI

### Key Components
- **JobManager**: Job coordination, TaskManager management, checkpointing (Port: 8088 HTTP, 50051 gRPC)
- **TaskManager**: Task execution, local state management, data exchange (Ports: 50070+)
- **Checkpointing**: Barrier-based fault tolerance with exactly-once semantics
- **Stream Processing API**: DataStream operations, operators, connectors

## Development Commands

### Building and Testing
```bash
# Build main solution
dotnet build FlinkDotNet/FlinkDotNet.sln

# Run unit tests
dotnet test FlinkDotNet/FlinkDotNet.sln -v minimal

# Run integration tests (requires Docker)
bash scripts/run-integration-tests-in-linux.sh          # Linux
pwsh scripts/run-integration-tests-in-windows-os.ps1    # Windows

# Start local development environment
dotnet run --project FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
```

### Local Development URLs
- **Aspire Dashboard**: http://localhost:18888
- **JobManager REST API**: http://localhost:8088  
- **JobManager gRPC**: http://localhost:50051

## Architecture Patterns

### gRPC Communication
- JobManager ↔ TaskManager communication via gRPC
- Protobuf message serialization
- Services inherit from generated ServiceBase classes
- Async/await patterns for all I/O operations

### State Management
- Implements FlinkDotnet's state semantics
- IValueState, IListState, IMapState interfaces
- StateDescriptor pattern for state registration
- Checkpoint barriers for fault tolerance

### Dependency Injection
- Microsoft.Extensions.DependencyInjection throughout
- Service registration in Program.cs/Startup.cs
- Constructor injection pattern

### Logging and Observability
- Microsoft.Extensions.Logging structured logging
- OpenTelemetry integration for distributed tracing
- Metrics collection for performance monitoring

## Development Guidelines

### Code Style
- Follow .NET naming conventions (PascalCase for public members)
- Use async/await for I/O operations
- Implement IDisposable for resource management
- Prefer composition over inheritance
- Use nullable reference types where appropriate

### Testing Strategy
- **Unit Tests**: Test individual components with mocked dependencies
- **Integration Tests**: Test with real Redis/Kafka using Aspire orchestration
- **Architecture Tests**: Verify dependency rules and structure
- Use xUnit framework with Moq for mocking

### Adding New Features

#### New Operators
1. Define interface in `FlinkDotNet.Core.Abstractions`
2. Implement in `FlinkDotNet.Core`
3. Add unit tests
4. Update documentation

#### New Connectors  
1. Implement source/sink interfaces
2. Handle serialization and exactly-once semantics
3. Add configuration options
4. Create integration tests with real systems

#### gRPC API Extensions
1. Update .proto files in appropriate project
2. Regenerate code using protoc
3. Implement service methods
4. Add client-side usage
5. Test inter-service communication

## Troubleshooting

### Common Issues
- **Build failures**: Ensure .NET 8 SDK is installed and restore NuGet packages
- **Integration test failures**: Verify Docker is running and ports are available
- **gRPC connection issues**: Check service discovery and port configurations

### Debugging Tips
- Use Aspire Dashboard to monitor service health and logs
- Check JobManager logs for task deployment issues
- Verify Redis/Kafka connectivity for integration tests
- Use .NET debugging tools for performance analysis

## AI-Assisted Development

### Custom Instructions
See `.copilot/instructions.md` for detailed Copilot instructions specific to this project.

### GitHub MCP Configuration
Model Context Protocol configuration in `.copilot/mcp-config.json` can be added to GitHub repository settings to provide GitHub Copilot with:
- Direct access to documentation and wiki content
- Enhanced project context and patterns
- Better understanding of Flink.NET architecture

To use this configuration:
1. Go to repository Settings → Copilot → Model Context Protocol
2. Add the configuration from `.copilot/mcp-config.json`

### Best Practices for AI Agents
- Reference existing patterns in the codebase
- Maintain consistency with established architecture
- Add comprehensive tests for new functionality
- Update documentation alongside code changes
- Consider performance implications for stream processing scenarios

## Integration Testing Details

Integration tests mimic `.github/workflows/integration-tests.yml` and require Docker for Aspire to spin up Redis and Kafka containers. The tests verify:
- End-to-end job submission and execution
- JobManager ↔ TaskManager communication
- Checkpointing and recovery scenarios
- Data flow through Redis and Kafka connectors

Use the provided scripts to reproduce the CI/CD workflow locally for faster development iteration.
