# TODO: Remaining Large Features and Enhancements

This document outlines significant features, enhancements, and complex TODO items that were identified during the initial assessment and require more substantial development effort. They are grouped by area for clarity.

## I. Core Runtime & Execution Engine

1.  **Full Windowing API Implementation:**
    *   **Description**: Implement the complete runtime logic for the Windowing API, including various window assigners, triggers, evictors, and process window functions. This includes managing window state, timers (event time and processing time), and watermark propagation.
    *   **Reference**: `docs/wiki/Developing-Windowing-Api.md`, `KeyedWindowProcessor.cs` (Snapshot/Restore TODOs).
    *   **Complexity**: High. Involves intricate state and timer management.

2.  **Event Time Processing & Watermarking:**
    *   **Description**: Fully integrate event time processing throughout the engine. This includes robust watermark generation at sources, propagation through operators, and handling by windowing logic and timers.
    *   **Reference**: `docs/flink_net_enhancements_and_simulation.md` (mentions lack of event time), `DataExchangeServiceImpl.cs` (Watermark TODO).
    *   **Complexity**: High.

3.  **State Backend Implementations (Durable):**
    *   **Description**: Develop and integrate production-ready durable state backends (e.g., RocksDB integration, or a custom solution for distributed storage like Azure Blob Storage/S3, potentially inspired by Flink's ForSt for disaggregated state).
    *   **Reference**: `docs/wiki/Core-Concepts-State-Management-Overview.md` (mentions "Durable Keyed State Backend (Planned)").
    *   **Complexity**: High.

4.  **Advanced Checkpointing Features:**
    *   **Checkpoint Timeout Logic**: Implement timers in `CheckpointCoordinator.cs` to fail checkpoints that don't complete within a configured timeout.
    *   **Checkpoint Cleanup**: Implement logic in `CheckpointCoordinator.cs` and `IJobRepository` for cleaning up old checkpoints based on retention policies.
    *   **Incremental Checkpointing**: Support for incremental checkpointing with state backends like RocksDB.
    *   **Asynchronous Snapshotting**: Optimize the snapshotting process to be more asynchronous and non-blocking for operators.
    *   **Reference**: `CheckpointCoordinator.cs` TODOs.
    *   **Complexity**: Medium to High.

5.  **Task Deployment Refactoring:**
    *   **Description**: Refactor the task deployment logic currently duplicated in `JobManagerController.cs` and `JobManagerInternalApiService.cs` into a shared, reusable service.
    *   **Reference**: TODO comments in `JobManagerInternalApiService.cs`.
    *   **Complexity**: Medium.

6.  **JobGraph Storage Refactoring:**
    *   **Description**: Implement proper storage of `JobGraph` instances via `IJobRepository` instead of the current static dictionary in `JobManagerController`. This might involve adding a `StoreJobGraphAsync(JobGraph jobGraph)` method to `IJobRepository`.
    *   **Reference**: TODO comment in `JobManagerInternalApiService.cs`.
    *   **Complexity**: Medium.

7.  **TaskManager Client Abstraction:**
    *   **Description**: Refactor TaskManager communication in `CheckpointCoordinator.cs` (and potentially task deployment logic) to use an injected client provider or proxy, instead of direct gRPC client instantiation using addresses from `TaskManagerTracker`.
    *   **Reference**: TODO comment in `CheckpointCoordinator.cs`.
    *   **Complexity**: Medium.

## II. JobManager Enhancements

1.  **High Availability (HA) for JobManager:**
    *   **Description**: Implement leader election, durable storage for JobManager metadata (JobGraphs, checkpoint metadata), and automated failover mechanisms, likely for Kubernetes deployments.
    *   **Reference**: `docs/wiki/Core-Concepts-JobManager.md` (HA section).
    *   **Complexity**: High.

2.  **Full Implementation of REST API Endpoints:**
    *   **Description**: Implement the functionality for all JobManager REST API endpoints currently returning 501 Not Implemented (GetJobStatus, ScaleJob, StopJob, CancelJob, RestartJob, DLQ management).
    *   **Reference**: `JobManagerController.cs`, `docs/wiki/JobManager-Rest-Api.md`.
    *   **Complexity**: Medium to High (depending on underlying feature completeness).

3.  **Job Lifecycle Orchestration:**
    *   **Description**: Implement full job lifecycle management logic in JobManager beyond basic deployment (e.g., robust failure detection, recovery coordination, scaling logic, graceful shutdown).
    *   **Reference**: `docs/wiki/Core-Concepts-JobManager.md` (Key Responsibilities).
    *   **Complexity**: High.

## III. Connectors

1.  **Production-Ready Kafka Connector:**
    *   **Description**: Develop a fault-tolerant Kafka source and sink that integrates with Flink.NET's checkpointing for exactly-once semantics, handles offset management, and supports robust configuration.
    *   **Reference**: `docs/wiki/Connectors-Overview.md`.
    *   **Complexity**: High.

2.  **Advanced File System Connectors:**
    *   **Description**: Create source and sink connectors for file systems (local, HDFS, S3, Azure Blob) with features like directory monitoring, partitioned writing, format support (CSV, JSON, Parquet), and checkpointing for file sources.
    *   **Reference**: `docs/wiki/Connectors-Overview.md`.
    *   **Complexity**: Medium to High.

3.  **Transactional Sinks (TwoPhaseCommitSinkFunction):**
    *   **Description**: Implement a base class and examples for transactional sinks that support two-phase commit protocols, enabling end-to-end exactly-once semantics with external systems.
    *   **Reference**: `docs/wiki/Connectors-Overview.md`.
    *   **Complexity**: High.

## IV. Observability

1.  **Comprehensive Metrics System:**
    *   **Description**: Implement a detailed metrics system (inspired by Apache Flink's metrics) for JobManager, TaskManager, and operators. Provide reporters for common monitoring systems (e.g., Prometheus).
    *   **Reference**: `docs/wiki/Advanced-Metrics-Monitoring.md`.
    *   **Complexity**: Medium to High.

2.  **Distributed Logging & Aggregation:**
    *   **Description**: Provide guidance and potentially tools/hooks for aggregating logs from distributed JobManager and TaskManager instances, especially in containerized environments.
    *   **Reference**: `docs/wiki/JobManager-Rest-Api.md` (GetJobLogs endpoint).
    *   **Complexity**: Medium.

## V. API and Usability Enhancements

1.  **StreamExecutionEnvironment Local Runner:**
    *   **Description**: Implement the `ExecuteAsync()` method in `StreamExecutionEnvironment.cs` to allow for local, in-process execution of Flink.NET jobs for testing and debugging, potentially with an embedded JobManager/TaskManager.
    *   **Reference**: TODO comment in `StreamExecutionEnvironment.cs`, `docs/wiki/Deployment-Local.md`.
    *   **Complexity**: Medium to High.

2.  **State TTL (Time-To-Live):**
    *   **Description**: Implement Time-To-Live (TTL) features for state, allowing automatic expiration and cleanup of state entries.
    *   **Reference**: `docs/wiki/Developing-State.md` (mentions State TTL as planned).
    *   **Complexity**: Medium.

## VI. Documentation (Beyond Initial Pass)

1.  **Complete All Placeholder Documentation:**
    *   **Description**: Fully write out all documentation pages that were created as placeholders (Windowing API, Kubernetes Deployment, specific connectors, advanced topics like Security, Performance Tuning, Metrics).
    *   **Reference**: `docs/wiki/Wiki-Structure-Outline.md`.
    *   **Complexity**: Medium (requires content creation once features are stable).

2.  **Create Developer Guides for Advanced Features:**
    *   **Description**: As advanced features (HA, new state backends, complex connectors) are implemented, create detailed developer guides for them.
    *   **Complexity**: Medium.

This list is not exhaustive but covers the major areas identified for further significant work.
