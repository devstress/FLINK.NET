# JobManager Internal gRPC API

[Back to Main Outline](./Wiki-Structure-Outline.md)

### Table of Contents
- [Introduction](#introduction)
- [Protocol Buffer Definitions](#protocol-buffer-definitions)
- [Key Services and RPCs](#key-services-and-rpcs)
  - [`JobManagerInternalService`](#jobmanagerinternalservice)
  - [`TaskManagerRegistration`](#taskmanagerregistration)
  - [Services Hosted by TaskManager (Called by JobManager or other TaskManagers)](#services-hosted-by-taskmanager-called-by-jobmanager-or-other-taskmanagers)
- [Usage](#usage)

---

## Introduction

The Flink.NET JobManager utilizes gRPC for internal communication, primarily between the JobManager and TaskManagers, and potentially for job submission from clients designed to use gRPC. These APIs are defined in `.proto` files and are generally not intended for direct use by end-users in the same way as the REST API.

**Note: The gRPC services are under active development, and their definitions and capabilities will evolve.**

## Protocol Buffer Definitions

The primary gRPC service definitions can be found in:
*   `FlinkDotNet/FlinkDotNet.JobManager/Protos/jobmanager_internal.proto`

This file defines services for:
*   Job Submission
*   Task Management and Deployment
*   Checkpointing Coordination
*   Heartbeating
*   Data Exchange (though `DataExchangeService` is hosted by TaskManagers)

## Key Services and RPCs

### `JobManagerInternalService`
*   Hosted by: JobManager
*   Purpose: Core internal operations related to job lifecycle and state.
*   Key RPCs (Conceptual / Implemented):
    *   `rpc SubmitJob (SubmitJobRequest) returns (SubmitJobReply)`: Submits a `JobGraph` (in its Protobuf representation) to the JobManager.
        *   Status: Implemented. The JobManager parses the `JobGraph`, stores it (currently in a static collection), initiates task deployment to registered TaskManagers, and starts a `CheckpointCoordinator`.
    *   `rpc ReportStateCompletion (ReportStateCompletionRequest) returns (ReportStateCompletionReply)`: Used by TaskManagers to report the completion of their state snapshot for a checkpoint.
        *   Status: Stubbed. (JobManager's `CheckpointCoordinator` expects this to be called).
    *   `rpc RequestCheckpoint (RequestCheckpointRequest) returns (RequestCheckpointReply)`: (May be deprecated or for specific scenarios) JobManager might request a TaskManager to initiate a checkpoint.
        *   Status: Stubbed.
    *   `rpc RequestRecovery (RequestRecoveryRequest) returns (RequestRecoveryReply)`: JobManager requests a TaskManager to recover.
        *   Status: Stubbed.

### `TaskManagerRegistration`
*   Hosted by: JobManager
*   Purpose: Handles TaskManager registration and heartbeating.
*   Key RPCs:
    *   `rpc RegisterTaskManager (RegisterTaskManagerRequest) returns (RegisterTaskManagerResponse)`: Allows a TaskManager to register itself with the JobManager.
        *   Status: Implemented. JobManager tracks registered TaskManagers.
    *   `rpc SendHeartbeat (HeartbeatRequest) returns (HeartbeatResponse)`: TaskManagers send periodic heartbeats to the JobManager. The request can include task metrics.
        *   Status: Implemented. JobManager updates heartbeat status and processes metrics.
    *   `rpc AcknowledgeCheckpoint(AcknowledgeCheckpointRequest) returns (AcknowledgeCheckpointResponse)`: TaskManagers acknowledge a successful checkpoint to the JobManager's `CheckpointCoordinator` via this service.
        *   Status: Implemented.

### Services Hosted by TaskManager (Called by JobManager or other TaskManagers)

While not hosted on the JobManager, these are critical parts of the gRPC ecosystem defined in the same `.proto` files:

*   **`TaskManagerCheckpointing`** (Hosted by TaskManager)
    *   `rpc TriggerTaskCheckpoint (TriggerCheckpointRequest) returns (TriggerCheckpointResponse)`: JobManager instructs a TaskManager to initiate a checkpoint for its tasks.
        *   Status: Implemented.
*   **`TaskExecution`** (Hosted by TaskManager)
    *   `rpc DeployTask (TaskDeploymentDescriptor) returns (DeployTaskResponse)`: JobManager sends a `TaskDeploymentDescriptor` to a TaskManager to deploy and start a task.
        *   Status: Implemented.
*   **`DataExchangeService`** (Hosted by TaskManager)
    *   `rpc ExchangeData (stream UpstreamPayload) returns (stream DownstreamPayload)`: Used for bi-directional streaming of data records and flow control credits between TaskManagers.
        *   Status: Implemented.

## Usage

*   **Job Submission**: Clients (like the `FlinkJobSimulator`) can use the `JobManagerInternalService.SubmitJob` RPC to submit pre-built `JobGraph` instances.
*   **Cluster Coordination**: The other services and RPCs are primarily used by Flink.NET's internal components (JobManager, TaskManager) to coordinate job execution, checkpointing, and maintain cluster health.

For detailed message formats and RPC signatures, please refer directly to the `.proto` files. The C# gRPC client and service base classes are generated from these definitions.

---
**Navigation**
*   Previous: [REST API](./JobManager-Rest-Api.md)
