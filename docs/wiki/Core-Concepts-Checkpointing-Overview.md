# Core Concepts: Checkpointing Overview

### Table of Contents
- [The Distributed Snapshotting Process](#the-distributed-snapshotting-process)
- [Role in Fault Tolerance & Recovery](#role-in-fault-tolerance--recovery)
- [Key Considerations](#key-considerations)

Checkpointing is fundamental to Flink.NET''s reliability. It is Flink.NET''s core mechanism for fault tolerance and exactly-once processing semantics. A checkpoint is a consistent snapshot of application state and stream positions. Apache Flink 2.0 continues to build upon this critical foundation, introducing architectural enhancements to further improve checkpointing efficiency and scalability.

See also: [State Management Overview](./Core-Concepts-State-Management-Overview.md)

*(Apache Flink Ref: [Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/))*

## The Distributed Snapshotting Process
Inspired by Apache Flink (Chandy-Lamport variant):

1.  **Triggering:** The **[JobManager''s](./Core-Concepts-JobManager.md) Checkpoint Coordinator** triggers checkpoints periodically.
2.  **Checkpoint Barriers:** Source Tasks inject barriers (with a Checkpoint ID) into data streams. Barriers flow inline with records.
3.  **Operator State Snapshotting:** When an operator receives barriers from all inputs, it snapshots its state to a durable state backend (e.g., MinIO, S3, Azure Blob) and forwards the barrier. Apache Flink 2.0's disaggregated state management, for instance, aims to make this snapshotting process to remote durable storage more efficient and faster, especially for applications with very large states in cloud environments, by leveraging asynchronous operations and optimized backends.
4.  **Alignment and Acknowledgement:** Multi-input operators align barriers. TaskManagers acknowledge snapshot completion (with state location) to the JobManager.
5.  **Checkpoint Completion & Metadata:** The JobManager marks a checkpoint "COMPLETED" upon receiving all acks and stores its metadata (`CheckpointMetadata`) durably.

## Role in Fault Tolerance & Recovery
If a failure occurs:
1.  JobManager stops the job.
2.  Selects the latest completed checkpoint from `CheckpointMetadata`.
3.  Redeploys tasks.
4.  Operators restore state from snapshots.
5.  Sources reset to checkpointed offsets.
6.  Processing resumes, ensuring exactly-once semantics.

## Key Considerations:
*   **State Backend Choice:** Impacts performance and reliability.
*   **Checkpoint Interval:** Balances recovery speed and overhead.
*   **Exactly-Once Sinks:** Often require two-phase commit coordinated with checkpoints.