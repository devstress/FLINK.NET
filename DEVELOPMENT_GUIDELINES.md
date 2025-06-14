# Development Guidelines for FLINK.NET Implementation

## Overview

This document provides comprehensive guidelines for developing and maintaining FLINK.NET as a full-featured Flink.Net like implementation for .NET and Kubernetes environments. Follow these guidelines to ensure consistency, quality, and compatibility with Flink.Net specifications.

## Pre-Commit Development Workflow

### 1. Local Build and Analysis

**ALWAYS** run the local build script before committing to catch warnings and errors early:

```bash
# For comprehensive local analysis (recommended)
./local-build-analysis.ps1

# For quick build without SonarCloud analysis  
./local-build-analysis.ps1 -SkipSonar

# Unix/Linux systems
./build-all-sln.sh
```

**Requirements:**
- Zero warnings and zero errors before committing
- All tests must pass
- SonarCloud analysis should show no new issues

### 2. Testing Strategy

Run tests in this order:

```bash
# 1. Unit tests (fastest)
dotnet test FlinkDotNet/FlinkDotNet.sln --configuration Release

# 2. Integration tests 
dotnet test FlinkDotNetAspire/FlinkDotNetAspire.IntegrationTests/ --configuration Release

# 3. Stress tests (manual trigger or CI)
# Check GitHub Actions stress-tests workflow
```

### 3. Code Quality Standards

- **No Magic Numbers**: Use constants from `FlinkDotNet.Common.Constants`
- **No Hardcoded URLs/Ports**: Use centralized configuration
- **Proper Logging**: Use structured logging with appropriate log levels
- **Resource Disposal**: Implement `IDisposable`/`IAsyncDisposable` properly
- **Exception Handling**: Comprehensive error handling with recovery strategies

## Flink.Net Architecture Implementation

### Core Components

1. **JobManager** (`FlinkDotNet.JobManager`)
   - **StateCoordinator**: Manages RocksDB state backends across TaskManagers
   - **BackPressureCoordinator**: Monitors system pressure and triggers dynamic scaling
   - **CheckpointCoordinator**: Coordinates distributed checkpoints with state backends
   - **TaskManagerOrchestrator**: Dynamic scaling of TaskManager instances

2. **TaskManager** (`FlinkDotNet.TaskManager`)
   - Stateful task execution with RocksDB state backend integration
   - Credit-based backpressure handling
   - Checkpoint participation and state snapshot creation

3. **State Management** (`FlinkDotNet.Storage.RocksDB`)
   - Production-ready RocksDB state backend
   - Column family support for organizing state
   - Performance monitoring and metrics collection
   - Distributed checkpoint coordination

### Design Patterns

#### 1. State Management Pattern

```csharp
// Create state coordinator in JobManager
var stateCoordinator = new StateCoordinator(logger, checkpointCoordinator);

// Register state backends for TaskManagers
var stateBackendId = await stateCoordinator.CreateStateBackendAsync(
    taskManagerId: "tm-001",
    jobId: "job-123", 
    config: new StateBackendConfig
    {
        StateDir = "/tmp/flink-state",
        ColumnFamilies = new[] { "default", "user_state", "operator_state" },
        WriteBufferSize = 64 * 1024 * 1024
    });
```

#### 2. Back Pressure and Scaling Pattern

```csharp
// Monitor system pressure and trigger scaling
var backPressureCoordinator = new BackPressureCoordinator(
    logger, stateCoordinator, taskManagerOrchestrator, 
    new BackPressureConfiguration
    {
        ScaleUpThreshold = 0.8,   // 80% pressure triggers scale up
        ScaleDownThreshold = 0.3, // 30% pressure triggers scale down
        MinTaskManagers = 1,
        MaxTaskManagers = 10
    });

// Scaling is automatic based on:
// - State backend pressure (write latency, memory usage)
// - Network pressure (throughput utilization)
// - CPU pressure (processing utilization)
// - Memory pressure (heap usage)
```

#### 3. Configuration Pattern

Always use environment variables with sensible defaults:

```csharp
// Good: Configurable with fallback
var checkpointInterval = int.TryParse(
    Environment.GetEnvironmentVariable("CHECKPOINT_INTERVAL_SECS"), 
    out var interval) ? interval : 30;

// Bad: Magic number
var checkpointInterval = 30;
```

## Kubernetes Integration

### 1. TaskManager Dynamic Scaling

The TaskManagerOrchestrator supports three deployment types:

```csharp
public enum TaskManagerDeploymentType
{
    Process,     // Local development
    Container,   // Docker environments  
    Kubernetes   // Production K8s clusters
}
```

### 2. Pod Manifest Generation

TaskManager pods are created with proper resource limits:

```yaml
resources:
  requests:
    memory: "1024Mi"
    cpu: "500m"
  limits:
    memory: "2048Mi" 
    cpu: "2000m"
```

### 3. Service Discovery

Use Kubernetes service discovery for JobManager-TaskManager communication:

```csharp
// Environment variables set by K8s
var jobManagerAddress = Environment.GetEnvironmentVariable("JOBMANAGER_RPC_ADDRESS") 
    ?? "flink-jobmanager:6123";
```

## Performance Optimization Guidelines

### 1. Memory Management

- Use `FlinkMemoryManager` for pooled memory allocation
- Implement off-heap memory for large datasets
- Monitor memory fragmentation and trigger GC when needed

### 2. State Backend Optimization

```csharp
var rocksDbConfig = new RocksDBConfiguration
{
    WriteBufferSize = 64 * 1024 * 1024, // 64MB for high throughput
    MaxBackgroundJobs = 4,               // Parallel compaction
    BlockBasedTableOptions = new BlockBasedTableOptions
    {
        BlockSize = 64 * 1024,           // 64KB blocks for SSD
        CacheSize = 256 * 1024 * 1024,   // 256MB cache
        BloomFilterBitsPerKey = 10       // Efficient key lookups
    }
};
```

### 3. Network Optimization

- Use credit-based flow control to prevent backpressure
- Implement network buffer pools for zero-copy operations
- Monitor network utilization and adjust buffer sizes

## Debugging and Monitoring

### 1. Logging Standards

```csharp
// Structured logging with context
_logger.LogInformation("TaskManager {TaskManagerId} scaled up. Total instances: {TotalCount}", 
    instanceId, GetTaskManagerCount());

// Performance-critical paths with debug level
_logger.LogDebug("Checkpoint {CheckpointId} created for state backend {StateBackendId}", 
    checkpointId, stateBackendId);

// Errors with full context
_logger.LogError(ex, "Failed to create state backend for TaskManager {TaskManagerId}, Job {JobId}", 
    taskManagerId, jobId);
```

### 2. Metrics Collection

Key metrics to monitor:

- **State Backend**: Memory usage, disk usage, write/read latency
- **Back Pressure**: Overall system pressure, scaling decisions
- **Checkpoints**: Success rate, duration, size
- **TaskManagers**: CPU/memory utilization, task count, network throughput

### 3. Health Checks

Implement comprehensive health checks:

```csharp
public class HealthStatus
{
    public string SystemHealth { get; set; } // HEALTHY, MODERATE, HIGH_PRESSURE, CRITICAL
    public double OverallPressure { get; set; } // 0.0 to 1.0
    public int ActiveTaskManagers { get; set; }
    public int ActiveStateBackends { get; set; }
}
```

## Common Issues and Solutions

### 1. State Backend Issues

- **Memory Pressure**: Monitor `StateMetrics.MemoryUsageBytes` and trigger scaling
- **Write Latency**: Check RocksDB configuration and disk performance
- **Checkpoint Failures**: Ensure proper state backend registration with coordinator

### 2. Scaling Issues

- **No Scale Up**: Check if max instances reached, verify pressure thresholds
- **Rapid Scaling**: Implement cooldown periods to prevent oscillation
- **Pod Creation Failures**: Verify Kubernetes permissions and resource quotas

## Future Development Priorities

### Phase 1: Core Stability
- [ ] Stress test dynamic scaling under high load
- [ ] Optimize RocksDB configuration for different workload patterns
- [ ] Implement checkpoint retention policies
- [ ] Add comprehensive monitoring dashboards

### Phase 2: Advanced Features  
- [ ] Table API with SQL query processing
- [ ] Watermark processing for event-time workflows
- [ ] Advanced windowing functions
- [ ] Exactly-once semantics for all connectors

### Phase 3: Enterprise Features
- [ ] Security framework (authentication, authorization)
- [ ] Multi-tenancy support
- [ ] Advanced monitoring and alerting
- [ ] Performance tuning recommendations

## Code Review Checklist

Before submitting PRs, ensure:

- [ ] Zero build warnings using `local-build-analysis.ps1`
- [ ] All tests pass (unit, integration, stress)
- [ ] No magic numbers or hardcoded values
- [ ] Proper error handling and logging
- [ ] Resource disposal implemented correctly
- [ ] Configuration uses environment variables with defaults
- [ ] Documentation updated for new features
- [ ] Performance impact considered and measured

## Support and Resources

- **Flink.Net Documentation**: https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/
- **FLINK.NET Wiki**: `docs/wiki/`
- **Architecture Documentation**: `docs/FLINKDOTNET_2_0_FEATURES.md`
- **Troubleshooting Guide**: Check stress test logs and GitHub Actions for specific error patterns

Following these guidelines ensures FLINK.NET maintains high quality, performance, and compatibility with Flink.Net standards while providing a superior .NET developer experience.
