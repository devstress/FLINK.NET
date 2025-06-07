# Core Concepts: Checkpointing - Barriers

Checkpointing is a fundamental mechanism in Flink.NET for achieving fault tolerance and exactly-once semantics. Checkpoint barriers are special messages injected into the data streams to trigger and coordinate the checkpointing process.

## Barrier Definition

A checkpoint barrier is a special marker that flows in-band with the data records through the job graph. When an operator receives a barrier, it understands that it should begin its part of the specified checkpoint.

### Barrier Message Structure

To integrate barriers into the existing data flow, the `DataRecord` message (defined in `Protos/jobmanager_internal.proto`) will be extended. This allows barriers to be sent over the same gRPC streams as regular data.

The proposed modification to `DataRecord` is:

```protobuf
message DataRecord {
  // Existing fields for routing and payload
  string target_job_vertex_id = 1;
  int32 target_subtask_index = 2;
  bytes payload = 3;
  // For regular data, this is the serialized user record.
  // For barriers, this field might be unused or could carry additional barrier-specific metadata if checkpoint_id and timestamp are not sufficient.

  // New Checkpoint Barrier Fields
  bool is_checkpoint_barrier = 4;     // If true, this DataRecord represents a checkpoint barrier.
  int64 checkpoint_id = 5;            // Unique ID for this checkpoint. Valid if is_checkpoint_barrier is true.
  int64 checkpoint_timestamp = 6;     // Timestamp when the checkpoint was initiated. Valid if is_checkpoint_barrier is true.
  // bytes checkpoint_options = 7;    // Optional: For future extensions like type of checkpoint (e.g., full, incremental) or other flags.
}
```

**Fields:**

*   **`is_checkpoint_barrier` (bool):** A flag that explicitly marks this `DataRecord` as a checkpoint barrier. If `false`, it's a regular data record.
*   **`checkpoint_id` (int64):** A unique identifier assigned to this specific checkpoint instance by the Checkpoint Coordinator in the JobManager. This ID is crucial for tracking the progress of a checkpoint across all parallel tasks and for recovery.
*   **`checkpoint_timestamp` (int64):** The timestamp (e.g., milliseconds since epoch) when the checkpoint was triggered by the JobManager. This can be useful for metrics, logging, and potentially for some advanced time-based operations or state versioning.
*   **`payload` (bytes):**
    *   For regular data records (`is_checkpoint_barrier = false`), this field contains the serialized user data.
    *   For checkpoint barriers (`is_checkpoint_barrier = true`), this field is typically not used for the primary barrier information (which is now in dedicated fields). However, it *could* be used in the future to carry additional barrier-specific metadata or options if needed, beyond what `checkpoint_options` might provide. For now, it can be empty for barriers.
*   **`checkpoint_options` (bytes, optional):** A placeholder for future enhancements. This could serialize a small message containing flags for different checkpoint types (e.g., "savepoint", "full checkpoint", "incremental hint") or other checkpoint-related instructions.

### Differentiation from Data Records

*   Operators and data exchange components will primarily check the `is_checkpoint_barrier` flag.
*   If `true`, the record is processed as a barrier, and `checkpoint_id` and `checkpoint_timestamp` are used.
*   If `false`, the record is processed as regular data, and `payload` is deserialized as user data.

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
Next: [Core Concepts: Serialization](./Core-Concepts-Serialization.md)
