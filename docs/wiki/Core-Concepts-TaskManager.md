# Core Concepts: TaskManager

### Table of Contents
- [Key Responsibilities](#key-responsibilities)
- [Deployment](#deployment)

TaskManagers (TMs) are the worker nodes in the Flink.NET architecture. They execute the data processing tasks of a job. A Flink.NET deployment typically consists of one or more TaskManagers, running in parallel, often as Kubernetes pods.

See also: [System Design Overview in Readme.md](../../../Readme.md#system-design-overview)

*(Apache Flink Ref: [TaskManager](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink_architecture/#taskmanager))*

## Key Responsibilities:

1.  **Task Execution:**
    *   Receives tasks from the [JobManager](./Core-Concepts-JobManager.md).
    *   Executes user-defined operator code (e.g., `IMapOperator.Map()`).
    *   Manages task lifecycle (`Open()`, `Close()` for Rich Functions).

2.  **Data Buffering and Exchange:**
    *   Manages network buffers for data shuffling between TaskManagers.

3.  **State Management (Local Aspect):**
    *   Manages actual state data for its tasks, interacting with the configured state backend.
    *   Snapshots state to durable storage during checkpointing.
    *   *(See Wiki Page: [Core Concepts: State Management Overview](./Core-Concepts-State-Management-Overview.md) for more details)*

4.  **Heartbeating and Status Reporting:**
    *   Sends heartbeats and task status updates to the JobManager.

5.  **Resource Management (Slots - Conceptual):**
    *   Offers resources (conceptually "task slots") for running parallel tasks.

## Deployment

*   TaskManagers run as Kubernetes Pods, enabling scaling.
*   The JobManager requests and assigns tasks to these pods.

*(Apache Flink Ref: [Task Execution & Scheduling](https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/task_scheduling/))*

TaskManagers are the workhorses of the Flink.NET system.
