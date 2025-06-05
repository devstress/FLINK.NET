# Core Concepts: TaskManager

TaskManagers (TMs) are the worker nodes in the Flink.NET architecture. They are responsible for executing the actual data processing tasks of a submitted job. A Flink.NET deployment typically consists of one or more TaskManagers, which run in parallel and can be distributed across multiple machines or Kubernetes pods.

*(Apache Flink Ref: [TaskManager](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink_architecture/#taskmanager))*

## Key Responsibilities:

1.  **Task Execution:**
    *   Receives tasks (parallel instances of operators from a job''s `ExecutionGraph`) deployed by the JobManager.
    *   Executes the user-defined code within these tasks (e.g., the logic inside `IMapOperator.Map()`, `IRichFilterOperator.Filter()`, etc.).
    *   Manages the lifecycle of these tasks, including setting them up (calling `Open()` on Rich Functions) and tearing them down (calling `Close()`).

2.  **Data Buffering and Exchange:**
    *   Manages network buffers for shuffling data between different TaskManagers when required by the job graph (e.g., after a `keyBy()` operation or for broadcast streams).
    *   Receives data from upstream tasks (which could be on the same or different TaskManagers) and sends data to downstream tasks.

3.  **State Management (Local Aspect):**
    *   For stateful operators, TaskManagers are responsible for managing the actual state data on behalf of the tasks they run.
    *   This involves interacting with the configured state backend (e.g., in-memory, embedded RocksDB, or a Redis-based store) to read, write, and update operator state.
    *   During checkpointing, TaskManagers instruct their operators to snapshot their state and then transmit these state snapshots to the durable object store designated by the JobManager.
    *   *(See Wiki Page: Core Concepts: State Management Overview for more details)*

4.  **Heartbeating and Status Reporting:**
    *   Sends regular heartbeats to the JobManager to indicate liveness.
    *   Reports the status of its assigned tasks (e.g., running, finished, failed) back to the JobManager.

5.  **Resource Management (Slots - Conceptual):**
    *   In Apache Flink, TaskManagers offer **task slots**, which are units of resource isolation. Each slot can run one parallel pipeline of tasks.
    *   Flink.NET will likely adopt a similar concept. When a TaskManager (as a Kubernetes Pod) starts, it will have a certain amount of resources (CPU, memory). The JobManager will assign tasks to these TaskManagers based on available resources/slots. The exact mechanism for slot definition and management in a K8s context will be detailed in the deployment section.

## Deployment

*   In Flink.NET, TaskManagers are designed to run as Kubernetes Pods. This allows for easy scaling of processing capacity by adjusting the number of TaskManager pod replicas.
*   The JobManager is responsible for requesting these pods from Kubernetes and assigning tasks to them.

*(Apache Flink Ref: [Task Execution & Scheduling](https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/task_scheduling/))*

TaskManagers are the workhorses of the Flink.NET system, performing all the data processing operations as dictated by the JobManager and the user''s job logic.
