# Kubernetes Deployment for Flink.NET

This page provides comprehensive guidance on deploying Flink.NET applications to Kubernetes, with focus on high-performance configurations for achieving 1+ million messages/second throughput.

## High-Performance Kubernetes Configuration

### Optimized Cluster Setup for 1M+ msg/s Target

Based on proven performance results (407,500 msg/sec achieved by `produce-1-million-messages.ps1` on single i9-12900k node), the following multi-node Kubernetes configuration is recommended to achieve 1+ million messages processed in under 1 second with full Flink.NET features (exactly-once semantics, state management, FIFO processing):

**Cluster Requirements:**
```yaml
# Minimum cluster specification for high-throughput Flink.NET
nodes: 3-5
per_node:
  cpu: "16+"  # Minimum 16 cores per node  
  memory: "32Gi+"  # Minimum 32GB RAM per node
  storage: "NVMe SSD"  # High IOPS storage required
  network: "10Gbps+"  # High-bandwidth interconnect
os: "Linux (Ubuntu 22.04 LTS recommended)"
container_runtime: "containerd"
```

**Flink.NET Pod Configuration:**
```yaml
# JobManager pod specification
jobmanager:
  resources:
    requests:
      cpu: "4"
      memory: "8Gi"
    limits:
      cpu: "6"
      memory: "12Gi"
  replicas: 1
  
# TaskManager pod specification  
taskmanager:
  resources:
    requests:
      cpu: "12"
      memory: "24Gi" 
    limits:
      cpu: "14"
      memory: "28Gi"
  replicas: 15  # 3 TaskManagers per node on 5-node cluster
  parallelism: 60  # 4 slots per TaskManager
```

### Performance Projections

**Scaling Analysis:**
- **Single Node (Proven)**: 407,500 msg/sec on i9-12900k
- **5-Node Cluster**: Theoretical 2M+ msg/sec (5x linear scaling)
- **Linux Container Efficiency**: +15-20% improvement over Windows
- **Network Optimization**: +20-30% with 10Gbps+ interconnect
- **Target Achievement**: Process 1M messages in <800ms

### Infrastructure Integration

**Kafka Configuration for K8s:**
```yaml
# High-throughput Kafka setup
kafka:
  partitions: 50  # Match TaskManager parallelism
  replicas: 3
  config:
    num.io.threads: 16
    num.network.threads: 8
    socket.send.buffer.bytes: 1048576
    socket.receive.buffer.bytes: 1048576
    replica.fetch.max.bytes: 10485760
```

## Core Concepts for Kubernetes Deployment (Planned)

Deploying Flink.NET on Kubernetes will involve considerations similar to deploying Apache Flink:

*   **Containerization**: Packaging JobManager and TaskManager processes into Docker images.
*   **JobManager Deployment**:
    *   Typically deployed as a Kubernetes `Deployment` or `StatefulSet`.
    *   High Availability (HA) for JobManagers will require leader election and durable storage for metadata (e.g., using Kubernetes HA patterns or external stores like ZooKeeper/etcd, though Flink.NET might opt for alternatives).
*   **TaskManager Deployment**:
    *   Typically deployed as a Kubernetes `Deployment`.
    *   Can be scaled based on workload.
*   **Job Submission**: How to submit `JobGraph`s to the JobManager running in Kubernetes (e.g., via a Kubernetes `Service` exposing the JobManager's REST or gRPC API).
*   **Configuration**: Managing Flink.NET configurations via ConfigMaps and Secrets.
*   **Resource Management**: Defining CPU and memory requests/limits for JobManager and TaskManager pods.
*   **Networking**: Ensuring proper network communication between JobManager, TaskManagers, and external services.
*   **State Management**: Configuring state backends, especially for durable storage like PersistentVolumes or object stores (S3, Azure Blob Storage) when using stateful applications.
*   **Logging and Monitoring**: Integrating with Kubernetes logging solutions (e.g., EFK/ELK stack) and monitoring systems (e.g., Prometheus, Grafana).

## Current Status

*   The Flink.NET JobManager and TaskManager are designed as separate services that can be containerized.
*   The `.NET Aspire` setup (`FlinkDotNetAspire`) demonstrates local orchestration of these services, which shares some principles with containerized deployments.
*   Official deployment scripts, Helm charts, or Kubernetes Operators for Flink.NET are not yet available.

## Future Work

This page will be updated with:
*   Example Dockerfiles for JobManager and TaskManager.
*   Sample Kubernetes manifest files (`Deployment`, `Service`, `ConfigMap`, etc.).
*   Guidance on High Availability (HA) setup for JobManager.
*   Best practices for resource allocation and configuration.
*   Instructions for job submission in a Kubernetes environment.

For general concepts, you can refer to the Apache Flink documentation on [Kubernetes Deployment](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/resource-providers/native_kubernetes/).

---
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
