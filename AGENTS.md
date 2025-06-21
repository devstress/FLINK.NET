# Flink.NET Development Guide for AI Agents

## Project Overview
Flink.NET is a .NET-native stream processing framework inspired by Apache Flink, providing stateful stream processing, exactly-once semantics, and fault tolerance within the .NET ecosystem.

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
- Implements Apache Flink like's state semantics
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

## 🚨 MANDATORY: AI Agent Stress Test Investigation Protocol

### Overview
All AI agents working on FLINK.NET issues MUST follow this investigation protocol, especially for stress test failures.

### Before Making Any Code Changes

**AI agents MUST perform these steps in order:**

1. **🔍 Run the Full Stress Test Pipeline**
   ```powershell
   # Execute the complete stress test to understand current state
   cd scripts
   .\run-simple-stress-test.ps1 -MessageCount 10000
   ```

2. **📋 Document Current Behavior**
   - Record exact error messages
   - Capture performance metrics  
   - Note infrastructure startup logs
   - Identify which components fail and which succeed

3. **🧪 Isolate Component Issues**
   ```powershell
   # Test individual components
   .\produce-1-million-messages.ps1 -MessageCount 1000
   .\wait-for-flinkjobsimulator-completion.ps1
   .\discover-aspire-ports.ps1
   ```

4. **🐳 Verify Infrastructure**
   ```bash
   # Check Docker containers
   docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}"
   
   # Test service connectivity
   docker exec <redis-container> redis-cli ping
   docker exec <kafka-container> kafka-topics --bootstrap-server localhost:9092 --list
   ```

### Investigation Requirements

**AI agents MUST investigate in this order:**

#### Phase 1: Infrastructure Verification
- [ ] **Redis Connection**: Verify Redis container starts and accepts connections
- [ ] **Kafka Broker**: Confirm Kafka container starts and API is accessible
- [ ] **Port Discovery**: Ensure dynamic port allocation works correctly
- [ ] **Network Connectivity**: Test inter-container communication

#### Phase 2: Data Flow Testing
- [ ] **Message Production**: Verify 1M messages can be produced successfully
- [ ] **Topic Creation**: Confirm Kafka topics exist with correct configuration
- [ ] **Consumer Registration**: Test FlinkJobSimulator can connect to Kafka
- [ ] **Redis Counter**: Verify Redis counters increment properly

#### Phase 3: Integration Testing
- [ ] **End-to-End Flow**: Test complete message pipeline
- [ ] **Performance Validation**: Achieve target throughput (1M+ msg/sec)
- [ ] **Error Handling**: Verify graceful failure modes
- [ ] **CI Compatibility**: Test in CI environment constraints

### Mandatory Code Quality Checks

**Before committing any changes:**

1. **🧪 Test Your Changes**
   ```powershell
   # Run modified stress test
   .\run-simple-stress-test.ps1 -MessageCount 10000
   
   # Verify expected metrics
   # ✅ Expected: >1M messages processed in <5 seconds (MANDATORY REQUIREMENT)
   # ✅ Expected: Target throughput: 200,000+ messages per second minimum
   # ✅ Expected: Redis counter increments correctly with exact accuracy
   # ✅ Expected: No hanging processes or timeout errors
   ```

2. **📊 Performance Regression Check**
   - Compare before/after performance metrics
   - Ensure no degradation in throughput
   - Verify memory usage remains acceptable

3. **🔒 CI Environment Testing**
   ```powershell
   # Test with CI constraints
   $env:CI = "true"
   .\run-simple-stress-test.ps1 -MessageCount 1000
   ```

### 🚨 ENFORCEMENT RULES

#### Absolute Requirements
1. **NO GUESSING**: All diagnoses must be based on actual test results
2. **FULL PIPELINE TESTING**: Always test the complete stress test pipeline
3. **MANUAL APPROACH MANDATORY**: Must follow manual stress testing approach from `docs/wiki/Stress-Tests-Overview.md`
4. **PERFORMANCE VALIDATION**: Changes must maintain or improve performance
5. **CI COMPATIBILITY**: All changes must work in CI environments

#### Mandatory Stress Testing Approach
**AI agents MUST follow the manual process exactly as documented:**

1. **Start Aspire AppHost**: `FlinkDotNetAspire.sln > F5`
2. **Wait for Services**: All Redis/Kafka/FlinkJobSimulator services running
3. **Produce Messages**: `cd scripts > .\produce-1-million-messages.ps1`
4. **Wait for Completion**: `.\wait-for-flinkjobsimulator-completion.ps1`
5. **If Docker Unavailable**: Do **not** start the AppHost or any Docker scripts.
   Ensure Kafka and Redis are running locally (ports `9092` and `6379`).
   Then open each Aspire service project and execute `dotnet run` manually:
   `FlinkJobSimulator`, `IntegrationTestVerifier`, `JobManager`, and `TaskManager`.
   This mirrors the AppHost startup without using Docker.

   Verify the ports are reachable (`kcat -b localhost:9092 -L`, `redis-cli PING`).

**NO automated scripts allowed** - must follow manual documented steps.

#### Performance Requirements
- **Processing Speed**: Messages must be processed in **< 5 seconds**
- **Redis Counter**: Must increment properly within timeout window
- **Throughput**: Target 1M+ messages/second capability
- **Kafka Broker**: Prefer locally installed Kafka or Apache official container when Docker is available

#### Forbidden Actions
❌ **Never do these:**
- Skip running the actual stress test
- Make changes based on assumptions
- Ignore performance regressions
- Break existing working functionality
- Commit without validating the fix

#### Quality Gates
**All changes must pass:**
- [ ] Stress test completes successfully
- [ ] Performance meets targets (1M+ msg/sec)
- [ ] No hanging processes or infinite loops
- [ ] CI environment compatibility
- [ ] Documentation is updated

## Integration Testing Details

Integration tests mimic `.github/workflows/integration-tests.yml` and require Docker for Aspire to spin up Redis and Kafka containers. The tests verify:
- End-to-end job submission and execution
- JobManager ↔ TaskManager communication
- Checkpointing and recovery scenarios
- Data flow through Redis and Kafka connectors

Use the provided scripts to reproduce the CI/CD workflow locally for faster development iteration.
