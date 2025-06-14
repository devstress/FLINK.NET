# Flink.NET Copilot Custom Instructions

## Project Overview
Flink.NET is a .NET-native stream processing framework inspired by FlinkDotnet. It provides stateful stream processing, exactly-once semantics, and fault tolerance within the .NET ecosystem.

## Key Architecture Components

### Core Services
- **JobManager**: Coordinates job execution, manages TaskManagers, handles checkpointing
- **TaskManager**: Executes tasks, manages local state, communicates via gRPC
- **Checkpointing**: Barrier-based fault tolerance with exactly-once semantics

### Technology Stack
- **.NET 8.0**: Primary runtime and framework
- **gRPC**: Inter-service communication between JobManager and TaskManagers  
- **Protobuf**: Message serialization for internal APIs
- **.NET Aspire**: Local development and orchestration
- **Redis & Kafka**: External systems for testing and examples
- **Docker**: Containerization for local development

### Project Structure
```
FlinkDotNet/                     # Core framework solution
├── FlinkDotNet.Core/           # Main implementation
├── FlinkDotNet.Core.Abstractions/ # Interfaces and contracts
├── FlinkDotNet.Core.Api/       # Public API layer
├── FlinkDotNet.JobManager/     # Job coordination service
├── FlinkDotNet.TaskManager/    # Task execution service
└── FlinkDotNet.*.Tests/        # Unit tests for each component

FlinkDotNetAspire/              # Local development and simulation
├── FlinkJobSimulator/          # Sample application
├── IntegrationTestVerifier/    # Integration testing
└── FlinkDotNetAspire.AppHost.AppHost/ # Aspire orchestration

FlinkDotNet.WebUI/              # Blazor WebAssembly UI
```

## Development Patterns

### Coding Conventions
- Use async/await patterns for I/O operations
- Implement IDisposable for resource management
- Follow .NET naming conventions (PascalCase for public members)
- Use structured logging with Microsoft.Extensions.Logging
- Leverage dependency injection throughout

### gRPC Service Implementation
- Services inherit from generated base classes
- Use ServerCallContext for request metadata
- Handle exceptions with proper gRPC status codes
- Implement both client and server sides for internal APIs

### State Management
- Implement IValueState, IListState, IMapState interfaces
- Use StateDescriptor for state registration
- Support checkpoint barriers for fault tolerance
- Consider serialization requirements for state types

### Operator Development
- Implement operator interfaces (IMapOperator, IFilterOperator, etc.)
- Support both simple and rich operator variants
- Use ICollector for output emission
- Implement IOperatorLifecycle for resource management

## Testing Approach

### Unit Tests
- Located alongside source code in *.Tests projects
- Use xUnit test framework
- Mock external dependencies
- Test individual components in isolation

### Integration Tests
- Use Aspire to orchestrate full system
- Test with real Redis and Kafka instances
- Verify end-to-end job execution
- Use scripts in `scripts/` directory for local testing

### Test Commands
```bash
# Unit tests
dotnet test FlinkDotNet/FlinkDotNet.sln -v minimal

# Integration tests (requires Docker)
bash scripts/run-integration-tests-in-linux.sh
pwsh scripts/run-integration-tests-in-windows-os.ps1
```

## Common Development Tasks

### Adding New Operators
1. Define interface in Core.Abstractions
2. Implement in Core project
3. Add to operator registry
4. Create unit tests
5. Update documentation

### Extending gRPC APIs
1. Update .proto files
2. Regenerate code using protoc
3. Implement service methods
4. Add client-side usage
5. Test communication

### Adding Connectors
1. Implement source/sink interfaces
2. Handle serialization properly
3. Support exactly-once semantics where applicable
4. Add configuration options
5. Create integration tests

## Build and Deployment

### Local Development
- Use .NET Aspire for orchestration
- Start with: `dotnet run --project FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost`
- Access Aspire Dashboard at http://localhost:18888
- JobManager REST API at http://localhost:8088
- JobManager gRPC at http://localhost:50051

### Project Status
- Currently in alpha/foundational development stage
- Focus on Phase 1: Core functionality for exactly-once FIFO processing
- Barrier-based checkpointing in development
- Performance optimization planned for later phases

## Important Files to Reference
- `README.md`: Project overview and getting started
- `AGENTS.md`: Development environment setup
- `docs/wiki/`: Comprehensive documentation
- `SONAR_EXCLUSIONS.md`: Code quality configuration
- Various workflow files in `.github/workflows/`

## When Contributing
- Reference existing patterns in the codebase
- Maintain backwards compatibility where possible  
- Add comprehensive tests for new functionality
- Update relevant documentation
- Follow the architecture established in existing components
- Consider performance implications for stream processing scenarios