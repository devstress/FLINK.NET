# Core Concepts: JobManager

The JobManager is the central coordinating component of a Flink.NET deployment. It is responsible for the orchestration of job execution, resource management (in conjunction with the underlying cluster manager like Kubernetes), checkpoint coordination, and failure recovery. In a high-availability setup, there can be multiple JobManager instances, but only one is active as the leader at any given time.

*(Apache Flink Ref: [JobManager](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink_architecture/#jobmanager))*

## Key Responsibilities:

1.  **Job Submission & Lifecycle Management:**
    *   Receives job submissions from clients (e.g., via the REST API). A job is typically submitted as a `JobGraph`, which is a description of the dataflow topology, operators, user code, and configuration.
    *   Manages the overall lifecycle of a job: deploying, monitoring, cancelling, stopping, and restarting.

2.  **Task Scheduling & Resource Allocation:**
    *   Translates the `JobGraph` into an `ExecutionGraph`, which represents the physical execution plan.
    *   Requests resources (e.g., TaskManager pods/slots) from the cluster manager (Kubernetes in Flink.NET''s target architecture).
    *   Assigns individual tasks (parallel instances of operators) to available TaskManagers.

3.  **Checkpoint Coordination:**
    *   The JobManager hosts the **Checkpoint Coordinator**.
    *   Triggers periodic checkpoints for running jobs.
    *   Receives acknowledgements from TaskManagers when they complete their part of a checkpoint.
    *   Manages metadata about completed checkpoints (e.g., storing it in a durable metadata store like Cosmos DB or PostgreSQL).
    *   Initiates recovery procedures using the last successfully completed checkpoint in case of failures.
    *   *(See Wiki Page: Core Concepts: Checkpointing Overview for more details)*

4.  **Failure Detection & Recovery:**
    *   Monitors TaskManagers via heartbeats (using the internal gRPC API).
    *   Detects failures of TaskManagers or individual tasks.
    *   Coordinates the recovery process, which involves:
        *   Cancelling remaining tasks from the failed execution attempt.
        *   Resetting tasks to the state of the last completed checkpoint.
        *   Re-requesting resources if needed and redeploying tasks.

5.  **Metadata & API Hosting:**
    *   Maintains metadata about running and completed jobs, task statuses, checkpoint history, etc.
    *   Exposes the **Job Management REST API** for external clients to interact with the system.
    *   Exposes the **Internal gRPC API** for communication with TaskManagers.

## Deployment & High Availability (HA)

*   In Flink.NET, the JobManager is designed to be deployed on Kubernetes.
*   For production environments, a High Availability (HA) setup is crucial. This typically involves running multiple JobManager instances with a leader election mechanism (e.g., using ZooKeeper, etcd, or Kubernetes leader election primitives). If the active leader fails, another standby JobManager takes over leadership, ensuring continuous operation by recovering job metadata from a durable store.

*(Apache Flink Ref: [High Availability (HA)](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/ha/))*

The JobManager is the "brain" of a Flink.NET application, ensuring that data processing jobs run smoothly, efficiently, and reliably.
