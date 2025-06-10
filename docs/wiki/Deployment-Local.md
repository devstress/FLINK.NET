# Local Development and Testing in Flink.NET

Setting up a local environment for developing and testing your Flink.NET applications is crucial for an efficient development workflow. Flink.NET provides a solution based on .NET Aspire to simplify this process.

## Using .NET Aspire for Local Environment (`FlinkDotNetAspire`)

The recommended way to run a local Flink.NET "cluster" along with necessary services (like Kafka and Redis for examples) is by using the .NET Aspire solution provided in the repository: `FlinkDotNetAspire`.

### Overview

The `FlinkDotNetAspire` solution includes:
*   **`AppHost` Project (`FlinkDotNetAspire.AppHost`)**: This is the main orchestrator. It defines and launches the different services that make up your local Flink.NET environment.
*   **Service Defaults Project (`FlinkDotNetAspire.ServiceDefaults`)**: Provides common configurations, such as OpenTelemetry setup, for the services.
*   **Flink Services**:
    *   `FlinkDotNet.JobManager`: Launches the JobManager process.
    *   `FlinkDotNet.TaskManager`: Launches one or more TaskManager processes (currently configured for one, `taskmanager1`).
*   **Job Simulator (`FlinkJobSimulator`)**: A sample client application that:
    *   Defines a Flink.NET streaming job using the `StreamExecutionEnvironment`.
    *   Builds a `JobGraph`.
    *   Submits the `JobGraph` to the local JobManager via gRPC.
    *   Includes example Source and Sink functions (e.g., interacting with Redis and Kafka).
*   **External Services (via Docker)**:
    *   **Redis**: Used by some examples for state or coordination (e.g., the `HighVolumeSourceFunction` in the simulator uses Redis for sequence ID generation).
    *   **Kafka**: Used by some examples as a data source or sink.

### How to Run

1.  **Prerequisites**:
    *   .NET SDK (latest, matching the project's requirements).
    *   .NET Aspire workload installed (`dotnet workload install aspire`).
    *   Docker Desktop running (Aspire uses Docker for Redis and Kafka containers).

2.  **Navigate to the AppHost project**:
    ```bash
    cd path/to/Flink.NET/FlinkDotNetAspire/FlinkDotNetAspire.AppHost
    ```

3.  **Run the AppHost**:
    ```bash
    dotnet run
    ```

4.  **Aspire Dashboard**:
    *   This command will typically launch the Aspire Dashboard in your web browser (e.g., `http://localhost:18888`).
    *   The dashboard allows you to view:
        *   The status of all orchestrated services (JobManager, TaskManager, Redis, Kafka, Simulator).
        *   Console logs for each service.
        *   Service endpoints and environment variables.
        *   Metrics and traces (if configured).

### Example Usage

The `docs/wiki/Sample-Local-High-Throughput-Test.md` provides a detailed walkthrough of running a specific high-throughput test using this Aspire setup, including how to observe data in Redis and Kafka. This sample demonstrates:
*   Job submission from `FlinkJobSimulator`.
*   Data flow through sources, operators, and sinks.
*   Interaction with external services like Redis and Kafka managed by Aspire.

## Conceptual Local Execution (In-Process)

The `StreamExecutionEnvironment` class contains an `ExecuteAsync(string jobName)` method. Conceptually, this method could, in the future, allow for a very lightweight local execution of a Flink.NET job within the same process, perhaps by embedding minimal versions of the JobManager and TaskManager logic.

**Current Status**:
*   The `ExecuteAsync()` method is currently a **placeholder**. It builds the `JobGraph` but does not execute it locally. The TODO comment inside this method reflects this:
    `// TODO: Implement local/embedded execution or connect to an embedded JobManager.`
    `// For distributed execution, serialize JobGraph and submit via gRPC (see FlinkJobSimulator for an example).`
*   For actual job execution, including local testing that simulates a distributed environment, the **.NET Aspire solution is the recommended approach.**

A true in-process local runner would be beneficial for very quick unit tests of operator logic or simple dataflows without the overhead of spinning up separate processes or Docker containers. This is a potential area for future development.

## Benefits of Aspire for Local Development

*   **Integrated Environment**: Manages all necessary Flink.NET components and external dependencies like Kafka/Redis in one place.
*   **Service Discovery**: Handles how services find each other (e.g., TaskManager finding JobManager).
*   **Observability**: Provides easy access to logs, metrics (via OpenTelemetry), and traces through the Aspire Dashboard.
*   **Simplified Configuration**: Manages connection strings and environment variables for services.

Using the `FlinkDotNetAspire` setup provides a robust and feature-rich environment for developing, testing, and debugging your Flink.NET applications locally.

For publishing the integration test image, follow the instructions in [GHCR Tokens and Workflow Dispatch](GHCR-Tokens.md).

---
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
