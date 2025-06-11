# Enterprise High Availability and Disaster Recovery

## Overview

FLINK.NET provides enterprise-grade High Availability (HA) and Cross-Region Disaster Recovery capabilities modeled after Apache Flink 2.0. These features ensure minimal downtime and data protection for mission-critical streaming applications.

## High Availability Features

### Leader Election

The `HighAvailabilityCoordinator` implements automatic leader election among JobManager instances:

- **Automatic Failover**: When the current leader fails, standby instances automatically compete for leadership
- **Health Monitoring**: Continuous heartbeat monitoring detects failed instances
- **Graceful Transitions**: Leadership changes are coordinated to prevent data loss
- **Configurable Timing**: Heartbeat intervals and timeouts can be tuned for different environments

```csharp
// Configure HA coordinator
var haConfig = new HighAvailabilityConfiguration
{
    HeartbeatInterval = TimeSpan.FromSeconds(5),
    LeaderElectionInterval = TimeSpan.FromSeconds(10),
    InstanceTimeout = TimeSpan.FromSeconds(30),
    CurrentRegion = "us-east-1",
    MinInstancesForElection = 1
};

var haCoordinator = new HighAvailabilityCoordinator(logger, haConfig);
```

### Instance Management

JobManager instances are automatically registered and managed:

```csharp
// Register instance in cluster
var instance = new JobManagerInstance
{
    InstanceId = "jm-primary-001",
    Region = "us-east-1", 
    IsHealthy = true,
    Endpoint = "https://jm-primary.example.com:8081"
};
await haCoordinator.RegisterInstanceAsync(instance);

// Monitor cluster status
var status = haCoordinator.GetClusterStatus();
Console.WriteLine($"Leader: {status.CurrentLeader}");
Console.WriteLine($"Healthy Instances: {status.HealthyInstances}/{status.TotalInstances}");
```

### Health Monitoring

The system continuously monitors instance health through:

- **Heartbeat Messages**: Regular health signals from each instance
- **Timeout Detection**: Automatic marking of unresponsive instances as unhealthy
- **Status Reporting**: Real-time cluster health visibility

## Cross-Region Disaster Recovery

### Multi-Region Coordination

The `CrossRegionFailoverCoordinator` manages JobManager clusters across geographic regions:

```csharp
var crossRegionConfig = new CrossRegionConfiguration
{
    PrimaryRegion = "us-east-1",
    KnownRegions = new List<string> { "us-east-1", "us-west-2", "eu-west-1" },
    HealthCheckInterval = TimeSpan.FromSeconds(30),
    ReplicationInterval = TimeSpan.FromMinutes(1),
    AutoFailoverEnabled = true
};

var failoverCoordinator = new CrossRegionFailoverCoordinator(logger, crossRegionConfig);
```

### Automatic Failover

When a primary region becomes unavailable:

1. **Detection**: Health checks identify region failure
2. **Selection**: Healthy standby region is selected for promotion
3. **Promotion**: Standby region becomes new primary
4. **Notification**: All components are notified of the change

```csharp
// Manual failover trigger
var success = await failoverCoordinator.TriggerFailoverAsync("us-west-2");
if (success)
{
    Console.WriteLine("Failover completed successfully");
}
```

### State Replication

Continuous state synchronization between regions ensures data consistency:

- **Checkpoint Replication**: State snapshots are replicated to standby regions
- **Configurable Intervals**: Replication frequency can be adjusted based on RPO requirements
- **Consistency Verification**: Automated checks ensure replicated data integrity

### Fail-Back Support

When the original primary region recovers:

```csharp
// Fail back to original primary
var failBackSuccess = await failoverCoordinator.FailBackAsync("us-east-1");
if (failBackSuccess)
{
    Console.WriteLine("Fail-back completed");
}
```

## Deployment Configurations

### Basic HA Setup

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager-ha
spec:
  replicas: 3  # Primary + 2 standby instances
  selector:
    matchLabels:
      app: flink-jobmanager
  template:
    spec:
      containers:
      - name: jobmanager
        image: flinkdotnet/jobmanager:2.0-ha
        env:
        - name: HA_ENABLED
          value: "true"
        - name: HA_HEARTBEAT_INTERVAL
          value: "5s"
        - name: HA_INSTANCE_TIMEOUT
          value: "30s"
```

### Cross-Region Setup

```yaml
# Primary Region (us-east-1)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager-east
  namespace: flink-system
spec:
  template:
    spec:
      containers:
      - name: jobmanager
        env:
        - name: CROSS_REGION_ENABLED
          value: "true"
        - name: CROSS_REGION_PRIMARY
          value: "us-east-1"
        - name: CROSS_REGION_KNOWN_REGIONS
          value: "us-east-1,us-west-2,eu-west-1"

---
# Standby Region (us-west-2)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager-west
  namespace: flink-system
spec:
  template:
    spec:
      containers:
      - name: jobmanager
        env:
        - name: CROSS_REGION_ENABLED
          value: "true"
        - name: CROSS_REGION_PRIMARY
          value: "us-east-1"
        - name: JOBMANAGER_ROLE
          value: "standby-region"
```

## Configuration Reference

### HighAvailabilityConfiguration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `HeartbeatInterval` | TimeSpan | 5s | Frequency of health check messages |
| `LeaderElectionInterval` | TimeSpan | 10s | How often to check for leader election |
| `InstanceTimeout` | TimeSpan | 30s | Time before marking instance unhealthy |
| `CurrentRegion` | string | "default" | Region identifier for this instance |
| `MinInstancesForElection` | int | 1 | Minimum instances required for election |

### CrossRegionConfiguration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `PrimaryRegion` | string | "us-east-1" | Primary region identifier |
| `KnownRegions` | List<string> | ["us-east-1"] | All regions in the cluster |
| `HealthCheckInterval` | TimeSpan | 30s | Frequency of region health checks |
| `ReplicationInterval` | TimeSpan | 1m | How often to replicate state |
| `RegionTimeout` | TimeSpan | 5m | Time before marking region unhealthy |
| `AutoFailoverEnabled` | bool | true | Enable automatic failover |

## Monitoring and Observability

### Cluster Status

```csharp
// Get HA cluster status
var haStatus = haCoordinator.GetClusterStatus();
Console.WriteLine($"Current Leader: {haStatus.CurrentLeader}");
Console.WriteLine($"Total Instances: {haStatus.TotalInstances}");
Console.WriteLine($"Healthy Instances: {haStatus.HealthyInstances}");

// Get cross-region status
var regionStatus = failoverCoordinator.GetClusterStatus();
Console.WriteLine($"Primary Region: {regionStatus.PrimaryRegion}");
Console.WriteLine($"Failover Active: {regionStatus.IsFailoverActive}");
Console.WriteLine($"Healthy Regions: {regionStatus.HealthyRegions}/{regionStatus.TotalRegions}");
```

### Health Check Endpoints

Both coordinators provide health information that can be exposed via REST APIs:

- `/health/ha` - HA cluster status
- `/health/regions` - Cross-region status
- `/metrics/ha` - HA performance metrics
- `/metrics/regions` - Cross-region metrics

## Best Practices

### HA Configuration

1. **Instance Count**: Deploy at least 3 JobManager instances for proper quorum
2. **Heartbeat Timing**: Set intervals based on network latency and acceptable failover time
3. **Resource Allocation**: Ensure standby instances have sufficient resources for promotion
4. **Persistent Storage**: Use shared storage for state that survives instance failures

### Cross-Region Setup

1. **Network Connectivity**: Ensure reliable connectivity between regions
2. **Replication Frequency**: Balance RPO requirements with network overhead
3. **Region Selection**: Choose geographically distributed regions for true disaster recovery
4. **Testing**: Regularly test failover and fail-back procedures

### Monitoring

1. **Alerting**: Set up alerts for leadership changes and region failures
2. **Dashboards**: Create visualizations for cluster health and failover history
3. **Log Aggregation**: Centralize logs for troubleshooting distributed failures
4. **Performance Monitoring**: Track failover times and replication lag

## Troubleshooting

### Common Issues

1. **Split-Brain**: Multiple leaders due to network partitions
   - **Solution**: Use proper quorum-based election algorithms
   - **Prevention**: Ensure reliable network connectivity

2. **Slow Failover**: Failover takes longer than expected
   - **Check**: Heartbeat intervals and timeouts
   - **Solution**: Tune timing parameters for your environment

3. **Replication Lag**: State replication falling behind
   - **Check**: Network bandwidth and replication interval
   - **Solution**: Increase replication frequency or optimize network

4. **Failed Fail-Back**: Cannot return to original primary region
   - **Check**: Primary region health and connectivity
   - **Solution**: Verify region is fully recovered before attempting fail-back

### Diagnostic Commands

```csharp
// Check instance health
var instances = haCoordinator.GetClusterStatus().Instances;
foreach (var instance in instances)
{
    Console.WriteLine($"{instance.InstanceId}: {(instance.IsHealthy ? "Healthy" : "Unhealthy")}");
}

// Check region connectivity
var regions = failoverCoordinator.GetClusterStatus().Regions;
foreach (var region in regions)
{
    Console.WriteLine($"{region.RegionId}: {(region.IsHealthy ? "Online" : "Offline")}");
}
```

## Performance Characteristics

| Metric | Typical Value | Notes |
|--------|---------------|-------|
| Failover Time | < 10 seconds | Local HA failover |
| Cross-Region Failover | < 30 seconds | Includes DNS propagation |
| Replication Lag | < 60 seconds | Depends on state size |
| Health Check Overhead | < 1% CPU | Minimal impact on performance |

This enterprise HA and disaster recovery system ensures FLINK.NET applications can maintain high availability and survive both local failures and regional disasters with minimal data loss and downtime.