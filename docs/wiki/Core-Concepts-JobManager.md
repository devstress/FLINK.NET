# Core Concepts: JobManager

### Table of Contents
- [Key Responsibilities](#key-responsibilities)
- [Deployment & High Availability (HA)](#deployment--high-availability-ha)

The JobManager is the central coordinating component of a Flink.NET deployment. It is responsible for the orchestration of job execution, resource management (in conjunction with the underlying cluster manager like Kubernetes), checkpoint coordination, and failure recovery. In a high-availability setup, there can be multiple JobManager instances, but only one is active as the leader at any given time.

See also: [System Design Overview in Readme.md](../../../Readme.md#system-design-overview)

*(Apache Flink Ref: [JobManager](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink_architecture/#jobmanager))*

## Key Responsibilities:

1.  **Job Submission & Lifecycle Management:**
    *   Receives job submissions from clients (e.g., via the REST API). A job is typically submitted as a `JobGraph`, which is a description of the dataflow topology, operators, user code, and configuration.
    *   Manages the overall lifecycle of a job: deploying, monitoring, cancelling, stopping, and restarting.

2.  **Task Scheduling & Resource Allocation:**
    *   Translates the `JobGraph` into an `ExecutionGraph`, which represents the physical execution plan.
    *   Requests resources (e.g., TaskManager pods/slots) from the cluster manager (Kubernetes in Flink.NET''s target architecture).
    *   Assigns individual tasks (parallel instances of operators) to available [TaskManagers](./Core-Concepts-TaskManager.md).

3.  **Checkpoint Coordination:**
    *   The JobManager hosts the **Checkpoint Coordinator**.
    *   Triggers periodic checkpoints for running jobs.
    *   Receives acknowledgements from TaskManagers when they complete their part of a checkpoint.
    *   Manages metadata about completed checkpoints (e.g., storing it in a durable metadata store).
    *   Initiates recovery procedures using the last successfully completed checkpoint in case of failures.
    *   *(See Wiki Page: [Core Concepts: Checkpointing Overview](./Core-Concepts-Checkpointing-Overview.md) for more details)*

4.  **Failure Detection & Recovery:**
    *   Monitors TaskManagers via heartbeats (using the internal gRPC API).
    *   Detects failures of TaskManagers or individual tasks.
    *   Coordinates the recovery process.

5.  **Metadata & API Hosting:**
    *   Maintains metadata about jobs, tasks, checkpoints, etc.
    *   Exposes the Job Management REST API and Internal gRPC API.

## Deployment & High Availability (HA)

*   In Flink.NET, the JobManager is designed to be deployed on Kubernetes.
*   Production environments require a High Availability (HA) setup (planned).

*(Apache Flink Ref: [High Availability (HA)](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/ha/))*

The JobManager is the "brain" of a Flink.NET application.
