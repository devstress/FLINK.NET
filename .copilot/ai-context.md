# Flink.NET AI Context Documentation

This document provides comprehensive context about the Flink.NET project for AI agents and developers.

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture Summary](#architecture-summary)
- [Key APIs and Services](#key-apis-and-services)
- [Development Patterns](#development-patterns)
- [Testing Strategy](#testing-strategy)
- [Current Implementation Status](#current-implementation-status)
- [Common Tasks and Examples](#common-tasks-and-examples)

## Project Overview

Flink.NET is an ambitious open-source project creating a .NET-native stream processing framework inspired by Apache Flink. The goal is to provide .NET developers with familiar tools for sophisticated real-time data processing.

**Key Goals:**
- Stateful stream processing with exactly-once semantics
- High throughput and low latency processing
- Fault tolerance through checkpointing
- Rich APIs for stream transformations
- Native .NET ecosystem integration

**Current Phase:** Alpha/foundational development focused on core functionality for exactly-once FIFO processing.

## Architecture Summary

### Core Components

#### JobManager
- **Role**: Central coordinator for job execution
- **Responsibilities**: 
  - Job submission and lifecycle management
  - TaskManager registration and monitoring
  - Checkpoint coordination
  - Resource allocation
- **Communication**: REST API (port 8088) and gRPC (port 50051)
- **Key Services**: JobManagerInternalApiService, TaskManagerRegistrationService

#### TaskManager  
- **Role**: Task execution engine
- **Responsibilities**:
  - Execute individual tasks/operators
  - Manage local state
  - Handle data exchange between tasks
  - Participate in checkpointing process
- **Communication**: gRPC (ports 50070+)
- **Key Services**: TaskExecution, TaskManagerCheckpointing, DataExchangeService

#### Stream Processing Model
- **DataStream**: Primary abstraction for data flows
- **Operators**: Transformations applied to data streams (Map, Filter, Reduce, etc.)
- **JobGraph**: Logical representation of the computation
- **Execution Graph**: Physical deployment plan with parallelism

### Technology Integration

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   JobManager    │◄──►│  TaskManager    │◄──►│  TaskManager    │
│   (Coordinator) │    │   (Executor)    │    │   (Executor)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └─────────────────gRPC Communication─────────────────┘
         │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Redis       │    │     Kafka       │    │   File System   │
│   (State/Sink)  │    │   (Source/Sink) │    │   (Source/Sink) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Key APIs and Services

### JobManager APIs

#### REST API (`JobManagerController`)
```csharp
// Job lifecycle operations
POST   /jobs                    // Submit new job
GET    /jobs/{jobId}            // Get job status  
DELETE /jobs/{jobId}            // Cancel job
POST   /jobs/{jobId}/restart    // Restart job
```

#### Internal gRPC API (`JobManagerInternalService`)
```protobuf
// Core job management
rpc SubmitJob(SubmitJobRequest) returns (SubmitJobReply);
rpc ReportStateCompletion(ReportStateCompletionRequest) returns (ReportStateCompletionReply);

// TaskManager coordination
rpc RegisterTaskManager(TaskManagerRegistrationRequest) returns (TaskManagerRegistrationReply);
rpc TaskManagerHeartbeat(HeartbeatRequest) returns (HeartbeatReply);
```

### TaskManager APIs

#### Task Execution (`TaskExecution`)
```protobuf
rpc DeployTask(TaskDeploymentDescriptor) returns (DeployTaskResponse);
```

#### Checkpointing (`TaskManagerCheckpointing`)  
```protobuf
rpc TriggerTaskCheckpoint(TriggerCheckpointRequest) returns (TriggerCheckpointResponse);
```

#### Data Exchange (`DataExchangeService`)
```protobuf
rpc ExchangeData(stream UpstreamPayload) returns (stream DownstreamPayload);
```

## Development Patterns

### Operator Implementation

```csharp
// Simple operator interface
public interface IMapOperator<TInput, TOutput>
{
    TOutput Map(TInput input);
}

// Rich operator with lifecycle and state access
public interface IRichMapOperator<TInput, TOutput> : IMapOperator<TInput, TOutput>, IOperatorLifecycle
{
    // Inherits Map method and lifecycle methods (Open, Close)
}

// Example implementation
public class AddOneOperator : IRichMapOperator<int, int>
{
    private IValueState<int> counterState;
    
    public void Open(IRuntimeContext context)
    {
        var descriptor = new ValueStateDescriptor<int>("counter", 0);
        counterState = context.GetState(descriptor);
    }
    
    public int Map(int input)
    {
        var count = counterState.Value();
        counterState.Update(count + 1);
        return input + 1;
    }
    
    public void Close()
    {
        // Cleanup resources
    }
}
```

### State Management Pattern

```csharp
// State descriptor registration
var stateDescriptor = new ValueStateDescriptor<MyData>("my-state", defaultValue);
var state = runtimeContext.GetState(stateDescriptor);

// State operations
var currentValue = state.Value();
state.Update(newValue);
state.Clear();
```

### gRPC Service Implementation

```csharp
public class MyGrpcService : MyServiceBase
{
    private readonly ILogger<MyGrpcService> _logger;
    
    public MyGrpcService(ILogger<MyGrpcService> logger)
    {
        _logger = logger;
    }
    
    public override async Task<MyResponse> MyMethod(MyRequest request, ServerCallContext context)
    {
        try
        {
            // Service implementation
            _logger.LogInformation("Processing request: {RequestId}", request.RequestId);
            
            var response = new MyResponse { Success = true };
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing request");
            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }
}
```

### Dependency Injection Setup

```csharp
// Program.cs
var builder = WebApplication.CreateBuilder(args);

// Add services
builder.Services.AddGrpc();
builder.Services.AddSingleton<IJobRepository, InMemoryJobRepository>();
builder.Services.AddScoped<IMyService, MyService>();

// Add logging
builder.Logging.AddConsole();

var app = builder.Build();

// Configure gRPC services
app.MapGrpcService<JobManagerInternalApiService>();
```

## Testing Strategy

### Unit Testing Pattern

```csharp
public class OperatorTests
{
    [Fact]
    public void MapOperator_TransformsInput_ReturnsExpectedOutput()
    {
        // Arrange
        var mockContext = new Mock<IRuntimeContext>();
        var mockState = new Mock<IValueState<int>>();
        mockContext.Setup(x => x.GetState(It.IsAny<ValueStateDescriptor<int>>()))
                  .Returns(mockState.Object);
        
        var operator = new AddOneOperator();
        operator.Open(mockContext.Object);
        
        // Act
        var result = operator.Map(5);
        
        // Assert
        Assert.Equal(6, result);
        mockState.Verify(x => x.Update(It.IsAny<int>()), Times.Once);
    }
}
```

### Integration Testing with Aspire

```csharp
public class EndToEndTests : IClassFixture<AspireFixture>
{
    private readonly AspireFixture _fixture;
    
    public EndToEndTests(AspireFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact]
    public async Task JobExecution_WithRedisAndKafka_CompletesSuccessfully()
    {
        // Arrange - Services started by AspireFixture
        var jobClient = _fixture.CreateJobManagerClient();
        
        // Act - Submit job
        var jobResult = await jobClient.SubmitJobAsync(CreateTestJob());
        
        // Assert - Verify completion
        Assert.True(jobResult.Success);
        await VerifyResultsInRedis();
    }
}
```

## Current Implementation Status

### Implemented Features ✅
- Core JobManager and TaskManager services with gRPC communication
- Basic stream processing API (StreamExecutionEnvironment, DataStream)
- JobGraph model and submission from client to JobManager  
- .NET Aspire project for local cluster simulation
- Initial observability setup using OpenTelemetry
- Experimental DisaggregatedStateBackend for local filesystem storage
- MonotonicWatermarkGenerator and SlidingEventTimeWindows
- HighAvailabilityCoordinator prototype for leader election

### In Development 🚧
- Barrier-based checkpointing for fault tolerance
- High-performance serializers and custom serializer support
- Full keyed processing logic with state management per key
- Operator chaining for performance optimization

### Planned Features 📋
- Production-ready state backends with remote storage
- Incremental checkpointing
- Memory optimizations and advanced flow control
- Additional connectors (File, Kafka, Database)
- Advanced windowing operations

## Common Tasks and Examples

### Adding a New Source Connector

1. **Define the interface** in `Core.Abstractions`:
```csharp
public interface IMySourceFunction<T> : ISourceFunction<T>
{
    void Configure(MySourceConfiguration config);
}
```

2. **Implement the source** in appropriate connector project:
```csharp
public class MySourceFunction : IMySourceFunction<MyData>
{
    private MySourceConfiguration _config;
    private volatile bool _isRunning = true;
    
    public void Configure(MySourceConfiguration config)
    {
        _config = config;
    }
    
    public async Task Run(ISourceContext<MyData> context)
    {
        while (_isRunning)
        {
            var data = await FetchData();
            context.Emit(data);
        }
    }
    
    public void Cancel()
    {
        _isRunning = false;
    }
}
```

3. **Add tests** and **update documentation**.

### Creating a Job Programmatically

```csharp
var env = StreamExecutionEnvironment.GetExecutionEnvironment();

var source = env.AddSource(new MySourceFunction())
                .SetName("my-source")
                .SetParallelism(4);

var processed = source.Map(new MyMapOperator())
                     .Filter(new MyFilterOperator())
                     .SetName("process-data");

processed.AddSink(new MySinkFunction())
         .SetName("my-sink");

await env.ExecuteAsync("My Stream Processing Job");
```

### Debugging Tips

1. **Use Aspire Dashboard** (http://localhost:18888) for service monitoring
2. **Check gRPC connectivity** between JobManager and TaskManagers
3. **Verify state serialization** when adding custom state types
4. **Monitor checkpoint progress** in JobManager logs
5. **Use structured logging** with correlation IDs for request tracing

This context should help AI agents understand the project structure, patterns, and current implementation status when contributing to Flink.NET development.