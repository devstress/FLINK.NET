# Sample: Local Flink.NET Setup & High Throughput Test (Redis Sequenced Messages to Kafka & Redis Sink)

This guide explains how to set up a local Flink.NET environment using the .NET Aspire simulation project. You'll run a high-throughput test that generates messages with sequence IDs from Redis, processes them, and then sends them to both a Kafka topic and a separate Redis counter sink. This demonstrates ordered processing and data flow to multiple sinks.

## Prerequisites

1.  **.NET 8 SDK:** Ensure you have the .NET 8 SDK installed.
2.  **.NET Aspire Workload:** Install the Aspire workload:
    ```bash
    dotnet workload install aspire
    ```
3.  **Docker:** Docker must be running, as Aspire will use it to launch Redis and Kafka containers.
4.  **Clone the Repository:** You need a local copy of the `Flink.NET` repository.
5.  **Kafka Tools (Optional):** To inspect Kafka messages, you might want Kafka's command-line tools or a GUI tool like Offset Explorer or Conduktor.

## Running the Local Flink.NET Cluster, Redis, Kafka, & Simulator

The `FlinkDotNetAspire` project orchestrates a local Flink.NET "cluster" (JobManager and TaskManager), Redis, Kafka, and the job simulator.

1.  **Navigate to AppHost Directory:**
    Open your terminal and navigate to the AppHost directory:
    ```bash
    cd path/to/Flink.NET/FlinkDotNetAspire/FlinkDotNetAspire.AppHost
    ```

2.  **Configure Simulation Parameters (Optional):**
    The `FlinkJobSimulator` behavior is configured via environment variables set in `FlinkDotNetAspire.AppHost/Program.cs`. You can modify these defaults:
    ```csharp
    // In FlinkDotNetAspire.AppHost/Program.cs for the "flinkjobsimulator" resource:
    .WithEnvironment("SIMULATOR_NUM_MESSAGES", "10000000") // For the 10 million message test
    .WithEnvironment("SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE", "flinkdotnet:global_sequence_id")
    .WithEnvironment("SIMULATOR_REDIS_KEY_SINK_COUNTER", "flinkdotnet:sample:processed_message_counter")
    .WithEnvironment("SIMULATOR_KAFKA_TOPIC", "flinkdotnet.sample.topic")
    ```
    *   `SIMULATOR_NUM_MESSAGES`: Number of messages to generate (default is 10,000 if not overridden here).
    *   `SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE`: Key used by the source to generate sequence IDs (will be reset to 0 by the source on startup).
    *   `SIMULATOR_REDIS_KEY_SINK_COUNTER`: Key used by `RedisIncrementSinkFunction` to count processed messages.
    *   `SIMULATOR_KAFKA_TOPIC`: Kafka topic for the `KafkaSinkFunction`.

    *Remember to rebuild the AppHost if you change its `Program.cs` (`dotnet build` in the AppHost directory).*

3.  **Run the Aspire AppHost:**
    Execute:
    ```bash
    dotnet run
    ```
    This will:
    *   Start Redis and Kafka containers.
    *   Start Flink.NET JobManager and TaskManager.
    *   Start the `FlinkJobSimulator`.
    *   Launch the .NET Aspire Dashboard (e.g., `http://localhost:18888`).

## Observing Behavior and Throughput

1.  **Console Logs:**
    *   **Aspire Dashboard:** Check logs for `redis`, `kafka`, `jobmanager`, `taskmanager1`, and `flinkjobsimulator`.
    *   `HighVolumeSourceFunction` (in `flinkjobsimulator` log): Will log connection to Redis, initialization of the global sequence key, and progress of emitting messages with sequence IDs.
    *   `RedisIncrementSinkFunction` (in `taskmanager1` log): Will log connection to Redis and its processed message count.
    *   `KafkaSinkFunction` (in `taskmanager1` log): Will log connection to Kafka and produced message count.

2.  **Aspire Dashboard Metrics (for TaskManager):**
    *   Navigate to the "Metrics" tab, select `taskmanager1`.
    *   Observe `flinkdotnet.taskmanager.records_sent` and `flinkdotnet.taskmanager.records_received`.

3.  **Observing Redis Data:**
    *   Find Redis port from Aspire Dashboard (`redis` service -> Endpoints).
    *   Using `redis-cli -p <PORT>`:
        *   **Global Sequence ID (Source):**
            ```bash
            GET flinkdotnet:global_sequence_id
            ```
            (Or your configured `SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE`). This shows the total number of sequence IDs generated. Should be equal to `SIMULATOR_NUM_MESSAGES`.
        *   **Processed Message Counter (Redis Sink):**
            ```bash
            GET flinkdotnet:sample:processed_message_counter
            ```
            (Or your configured `SIMULATOR_REDIS_KEY_SINK_COUNTER`). This shows how many messages reached the Redis sink. Should also equal `SIMULATOR_NUM_MESSAGES`.

4.  **Observing Kafka Messages:**
    *   Find Kafka bootstrap server address from Aspire Dashboard (`kafka` service -> Environment variables, look for `KAFKA_ADVERTISED_LISTENERS` or similar, typically `localhost:<dynamic_port>`).
    *   Using Kafka console consumer tool (if installed). Example (replace bootstrap server and topic):
        ```bash
        kafka-console-consumer.sh --bootstrap-server localhost:XXXXX --topic flinkdotnet.sample.topic --from-beginning
        ```
        (Or your configured `SIMULATOR_KAFKA_TOPIC`). You should see messages like "MessagePayload_Seq-1", "MessagePayload_Seq-2", etc.

5.  **Observing Checkpoint Barrier Flow (Proof-of-Concept):**
    The current PoC uses special string markers to simulate barrier flow. Check the logs for:
    *   **`flinkjobsimulator` logs:**
        *   `HighVolumeSourceFunction`: Look for messages like:
            *   `Injecting Barrier: BARRIER_{id}_{timestamp}`
            *   `Injecting Final Barrier: BARRIER_{id}_{timestamp}_FINAL`
    *   **`taskmanager1` logs:**
        *   `NetworkedCollector`: (Prefixed with source ID and subtask index, e.g., `[jobVertexId_0]`)
            *   `NetworkedCollector received string barrier marker: BARRIER_{id}_{timestamp}`
            *   `Converted string marker to Protobuf Barrier ID: {id}`
        *   `DataExchangeServiceImpl`: (Prefixed with TaskManager ID, e.g., `[DataExchangeService-tm-1]`)
            *   `Received PROTOFBUF BARRIER for {targetVertexId}_{targetSubtaskIndex}. ID: {id}, Timestamp: {timestamp}`
        *   Sink Functions (`RedisIncrementSinkFunction`, `KafkaSinkFunction`): (Prefixed with task name)
            *   `Received Barrier Marker in Redis Sink: PROTO_BARRIER_{id}_{timestamp}`
            *   `Received Barrier Marker in Kafka Sink: PROTO_BARRIER_{id}_{timestamp}`

    These log entries help confirm the PoC barrier markers are being injected by the source, converted to Protobuf barriers by the `NetworkedCollector`, received as Protobuf barriers by `DataExchangeService`, reconverted to string markers, and finally observed by the sinks.

## Expected Behavior

*   **Sequence Generation:** `HighVolumeSourceFunction` connects to Redis, sets `SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE` to `0`, then `INCR`s it for each message, generating sequence IDs from 1 up to `SIMULATOR_NUM_MESSAGES`.
*   **Job Execution:** The Flink.NET job (Source -> Map -> Redis Sink AND Kafka Sink) processes these messages.
*   **Ordered Processing:** For this P=1 pipeline, Flink.NET processes messages in FIFO order.
*   **Redis Sink:** The `RedisIncrementSinkFunction` increments `SIMULATOR_REDIS_KEY_SINK_COUNTER` for each message. Its final value should match `SIMULATOR_NUM_MESSAGES`.
*   **Kafka Sink:** The `KafkaSinkFunction` sends messages (e.g., "MessagePayload_Seq-1", "MessagePayload_Seq-2", ...) to the `SIMULATOR_KAFKA_TOPIC`. These messages in Kafka will contain the Redis-generated sequence IDs, demonstrating the order.
*   **Metrics:** TaskManager metrics on Aspire Dashboard will show records being sent and received.
*   **Final State:**
    *   Redis key `SIMULATOR_REDIS_KEY_GLOBAL_SEQUENCE` should equal `SIMULATOR_NUM_MESSAGES`.
    *   Redis key `SIMULATOR_REDIS_KEY_SINK_COUNTER` should equal `SIMULATOR_NUM_MESSAGES`.
    *   The Kafka topic should contain all messages with ordered sequence IDs.

## Troubleshooting

*   **Docker Not Running:** Aspire needs Docker for Redis/Kafka.
*   **Connection Issues (Redis/Kafka):** Check service logs in Aspire Dashboard. Ensure correct connection strings/keys/topics are used (Aspire injects these as environment variables; `ConnectionStrings__redis`, `ConnectionStrings__kafka`).
*   Refer to general Flink.NET and Aspire troubleshooting if services don't start.
