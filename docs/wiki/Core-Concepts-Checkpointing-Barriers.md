# Core Concepts: Checkpointing - Barriers

Checkpointing is a fundamental mechanism in Flink.NET for achieving fault tolerance and exactly-once semantics. Checkpoint barriers are special messages injected into the data streams to trigger and coordinate the checkpointing process.

## Barrier Definition

A checkpoint barrier is a special message that flows in-band with the data records through the job graph. When an operator receives a barrier, it understands that it should begin its part of the specified checkpoint.

### Barrier Message Structure

Checkpoint barriers are integrated into the data stream using the `DataRecord` message defined in `Protos/jobmanager_internal.proto`. This allows barriers to be sent over the same gRPC streams as regular data records.

The `DataRecord` structure includes specific fields to designate a message as a barrier and carry checkpoint information:

```protobuf
message CheckpointBarrier {
  int64 checkpoint_id = 1;
  int64 checkpoint_timestamp = 2;
}

message DataRecord {
    string targetJobVertexId = 1;     // Target JobVertex on the receiving TM
    int32 targetSubtaskIndex = 2;     // Specific subtask index of the target operator

    string source_job_vertex_id = 5;  // JobVertexId of the sending task
    int32 source_subtask_index = 6;   // SubtaskIndex of the sending task

    oneof payload_type {
      bytes data_payload = 3;           // Serialized user data record
      CheckpointBarrier barrier_payload = 4; // Contains id and timestamp for a barrier
      Watermark watermark_payload = 7;    // For event time watermarks
    }

  bool is_checkpoint_barrier = 8; // If true, this DataRecord represents a checkpoint barrier,
                                  // and barrier_payload is expected to be set.
  bytes checkpoint_options = 9;   // Optional: For future extensions like type of checkpoint or other flags
}
```

**Key Fields for Barriers:**

*   **`is_checkpoint_barrier` (bool, field 8):** Explicitly marks the `DataRecord` as a checkpoint barrier.
*   **`barrier_payload` (CheckpointBarrier, field 4 within `oneof`):** If `is_checkpoint_barrier` is true, this field is populated. It's a nested message containing:
    *   **`checkpoint_id` (int64):** Unique ID for the checkpoint, assigned by the JobManager's Checkpoint Coordinator.
    *   **`checkpoint_timestamp` (int64):** Timestamp (ms since epoch) when the checkpoint was initiated.
*   **`data_payload` (bytes, field 3 within `oneof`):** Used for actual user data when `is_checkpoint_barrier` is false. For barriers, this is not set.
*   **`checkpoint_options` (bytes, field 9):** Reserved for future enhancements, like specifying checkpoint types (e.g., savepoint, incremental).

### Differentiation from Data Records

Data processing components check the `is_checkpoint_barrier` flag.
*   If `true`, the record is processed as a barrier, using the information from `barrier_payload`.
*   If `false`, the `data_payload` is deserialized and processed as a regular user record.

## Barrier Serialization

*   Serialization is handled by Protobuf due to the definition within the `.proto` file.
*   The `NetworkBuffer.cs` in C# already contains an `IsBarrierPayload` property. This property in the C# `NetworkBuffer` object (which wraps the byte array for network transfer or in-memory exchange) should be set to `true` when a `DataRecord` representing a barrier is being serialized into it, or read from it. This allows in-memory components to quickly identify buffers containing barriers without fully deserializing the Protobuf message if not necessary.

## Proof-of-Concept Barrier Flow (String Markers)

To test the end-to-end flow of barrier-like messages without immediately altering core operator logic or `ProcessRecordDelegate` signatures, a simplified Proof-of-Concept (PoC) mechanism using special string markers has been implemented in the `FlinkJobSimulator` and related TaskManager components.

**This PoC demonstrates the *concept* of barrier propagation but does not yet implement actual state snapshotting based on these barriers.**

1.  **Barrier Injection at the Source (`HighVolumeSourceFunction`):**
    *   The `HighVolumeSourceFunction` (in `FlinkDotNetAspire/FlinkJobSimulator/Program.cs`) is configured to inject a special string marker periodically (e.g., every 1000 data messages).
    *   This marker is a simple string formatted as: `"BARRIER_{checkpointId}_{timestamp}"`.
    *   The `checkpointId` is a counter incremented by the source, and `timestamp` is the current UTC milliseconds.
    *   This string marker is passed to `ctx.Collect()`, just like regular data messages.

2.  **Conversion to Protobuf Barrier (`NetworkedCollector`):**
    *   The `NetworkedCollector` (in `FlinkDotNet.TaskManager.TaskExecutor.cs`) receives records from the source function via the `ISourceContext.Collect()` mechanism, which ultimately calls `NetworkedCollector.Collect(T record, ...)`.
    *   Inside `NetworkedCollector.Collect`, it checks if the incoming `record` is a string that starts with `"BARRIER_"`.
    *   If it matches, the `NetworkedCollector` parses this string to extract the `checkpointId` and `timestamp`.
    *   It then constructs a proper `FlinkDotNet.Proto.Internal.DataRecord` Protobuf message with:
        *   `is_checkpoint_barrier = true`.
        *   The `barrier_payload` field (a `CheckpointBarrier` message) populated with the parsed `checkpointId` and `timestamp`.
        *   The main `payload` field of the `DataRecord` is set to `ByteString.Empty`.
    *   This formal `DataRecord` (now a true Protobuf barrier) is then sent over the gRPC stream to the downstream TaskManager.

3.  **Reception and Re-Conversion at Data Exchange (`DataExchangeServiceImpl`):**
    *   The `DataExchangeServiceImpl` on the receiving TaskManager receives the `DataRecord` Protobuf message.
    *   It checks the `is_checkpoint_barrier` flag on the incoming `DataRecord`.
    *   If `true`, it logs the reception of this "PROTOBUF BARRIER" along with its ID and timestamp from `dataRecord.BarrierPayload`.
    *   **PoC Simplification:** To avoid changing the `ProcessRecordDelegate` signature (which expects `byte[] payload`) and subsequent operator logic immediately, `DataExchangeServiceImpl` then *converts this Protobuf barrier back into a special string marker*.
    *   This new string marker is formatted like: `"PROTO_BARRIER_{checkpointId}_{timestamp}"`.
    *   This string is then UTF-8 encoded into a `byte[]` and passed to the `ProcessRecordDelegate`.

4.  **Observation at the Sink (PoC Sinks):**
    *   The PoC sinks (`RedisIncrementSinkFunction` and `KafkaSinkFunction` in `FlinkJobSimulator`) receive records via the `ProcessRecordDelegate`.
    *   Their `Invoke` methods check if the incoming record (when deserialized or cast to a string) starts with `"PROTO_BARRIER_"`.
    *   If it is such a marker, they log its reception and then typically `return`, effectively ignoring it for data processing purposes (e.g., not incrementing a counter or sending to Kafka).

**Rationale for PoC String Marker Approach:**

*   This mechanism allows testing the flow of barrier-like signals from source, through the network stack (as proper Protobuf barriers), and to the sinks.
*   It avoids requiring immediate, complex changes to the `TaskExecutor`'s operator-running logic, the `ProcessRecordDelegate` signature, or how individual operators handle different payload types (data vs. true barriers).
*   It serves as an observable placeholder for where true barrier handling logic (like triggering state snapshots) would eventually be integrated into operators and sinks.

The actual `DataRecord` Protobuf definition (with `is_checkpoint_barrier`, `barrier_payload`, etc.) is the target structure for true checkpointing. The string marker flow is a temporary bridge for this PoC.

## Next Steps (Runtime Implementation - Not part of this design document)

*   **Barrier Injection:** Source tasks will need to receive commands from the JobManager (Checkpoint Coordinator) to initiate a checkpoint. Upon receiving such a command, they will generate these barrier `DataRecord` messages and send them downstream.
*   **Barrier Propagation:**
    *   `NetworkedCollector` and `DataExchangeServiceImpl` must ensure barriers are not overtaken by data within their buffers and are promptly forwarded.
    *   Barriers must be broadcast to all downstream outputs from an operator.
*   **Barrier Alignment:** Operators with multiple inputs must align barriers from all inputs before processing their own state snapshot for that checkpoint.
*   **State Snapshotting:** Upon receiving aligned barriers (or a barrier on a single input operator), operators will snapshot their state to the configured state backend.
*   **Acknowledgement:** Tasks acknowledge successful state snapshotting to the JobManager.
*   **Checkpoint Coordination:** The JobManager tracks barrier progress and acknowledgements to declare a checkpoint complete or failed.

This initial design focuses on the structure and identification of barriers. The runtime behavior is complex and will be detailed in subsequent design and implementation phases.

---
Previous: [Core Concepts: Checkpointing Overview](./Core-Concepts-Checkpointing-Overview.md)
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
Next: [Core Concepts: Serialization](./Core-Concepts-Serialization.md)
