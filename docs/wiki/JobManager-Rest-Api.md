# JobManager REST API

[Back to Main Outline](./Wiki-Structure-Outline.md)

### Table of Contents
- [Introduction](#introduction)
- [API Endpoints (Conceptual / In-Progress)](#api-endpoints-conceptual--in-progress)
- [Authentication and Authorization](#authentication-and-authorization)
- [Further Development](#further-development)

---

## Introduction

The Flink.NET JobManager exposes a REST API for submitting jobs, monitoring their status, and managing the cluster. This API is intended for use by client tools, dashboards, and potentially for programmatic interaction.

**Note: The JobManager REST API is currently under development. Some endpoints may exist as placeholders, and functionality will be expanded over time.**

## API Endpoints (Conceptual / In-Progress)

The following is a conceptual list of endpoints based on common Flink patterns and current implementation stubs in `FlinkDotNet.JobManager.Controllers.JobManagerController.cs`. The base path is typically `/api/jobmanager/`.

*   **Job Management:**
    *   `POST /api/jobmanager/submit`: Submits a job for execution.
        *   **Request Body**: `JobDefinitionDto` (see `FlinkDotNet.JobManager.Models.JobDefinitionDto.cs`). Currently supports a linear chain of source, operators, and sink.
        *   **Status**: Partially implemented (accepts a simplified job definition, initiates deployment to available TaskManagers).
    *   `GET /api/jobmanager/jobs`: Lists all jobs and their overview.
        *   **Status**: Implemented (shows jobs from in-memory store).
    *   `GET /api/jobmanager/jobs/{jobId}`: Retrieves detailed information about a specific job (e.g., its `JobGraph`).
        *   **Status**: Implemented (shows `JobGraph` from in-memory store).
    *   `GET /api/jobmanager/jobs/{jobId}/status`: Retrieves the current status of a specific job.
        *   **Status**: Placeholder (returns 501 Not Implemented).
    *   `GET /api/jobmanager/jobs/{jobId}/metrics`: Retrieves runtime metrics for a specific job and its vertices.
        *   **Status**: Implemented (aggregates metrics from `TaskInstanceMetrics` in the `JobGraph` model).
    *   `POST /api/jobmanager/jobs/{jobId}/stop`: Stops a running job.
        *   **Status**: Placeholder (returns 501 Not Implemented).
    *   `POST /api/jobmanager/jobs/{jobId}/cancel`: Cancels a running job.
        *   **Status**: Placeholder (returns 501 Not Implemented).
    *   `POST /api/jobmanager/jobs/{jobId}/restart`: Restarts a completed or failed job (potentially from a checkpoint).
        *   **Status**: Placeholder (returns 501 Not Implemented).
    *   `PUT /api/jobmanager/jobs/{jobId}/scale`: Scales a job or specific operators.
        *   **Request Body**: `ScaleParametersDto`.
        *   **Status**: Placeholder (returns 501 Not Implemented).

*   **Checkpointing:**
    *   `GET /api/jobmanager/jobs/{jobId}/checkpoints`: Lists checkpoint history for a job.
        *   **Status**: Implemented (retrieves from `IJobRepository`).

*   **TaskManagers:**
    *   `GET /api/jobmanager/taskmanagers`: Lists all registered TaskManagers and their status.
        *   **Status**: Implemented (retrieves from static `TaskManagerTracker`).

*   **Logging & DLQ (Dead Letter Queue):**
    *   `GET /api/jobmanager/jobs/{jobId}/logs`: Retrieves logs for a specific job.
        *   **Status**: Basic implementation (returns a message indicating job-specific log aggregation is not fully implemented).
    *   `POST /dlq/{jobId}/resubmit`: Resubmits messages from a job's DLQ.
        *   **Status**: Placeholder (returns 501 Not Implemented).
    *   `PUT /dlq/{jobId}/messages/{messageId}`: Modifies a specific message in a job's DLQ.
        *   **Request Body**: `DlqMessageDto`.
        *   **Status**: Placeholder (returns 501 Not Implemented).

## Authentication and Authorization

(Details TBD - Security features are planned for future development).

## Further Development

This API will be expanded with more endpoints and richer functionality as Flink.NET matures. Refer to the source code (`JobManagerController.cs`) and future API specifications for the most up-to-date information.

---
**Navigation**
*   Next: [Internal gRPC API](./JobManager-Grpc-Api.md)
