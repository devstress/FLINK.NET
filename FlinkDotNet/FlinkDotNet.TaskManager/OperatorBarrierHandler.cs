#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Models.Checkpointing; // For CheckpointBarrier (the abstraction model)
using FlinkDotNet.TaskManager; // For TaskExecutor.NetworkedCollector (if used directly, or pass Action<object>)
                                  // For now, it will return a boolean indicating if snapshot should be triggered.

namespace FlinkDotNet.TaskManager
{
    public class OperatorBarrierHandler
    {
        private class BarrierState
        {
            public long CheckpointId { get; }
            public long CheckpointTimestamp { get; private set; } // Timestamp of the first barrier for this CP
            public HashSet<string> ReceivedFromInputChannels { get; } = new();

            // This field is part of the class but was not used in the provided snippet's IsAligned logic directly.
            // It's crucial for IsAligned to work correctly. Assuming it should be set in BarrierState constructor or similar.
            // For now, I'll make IsAligned take it as a parameter from the outer class.
            // public bool IsAligned => ExpectedInputChannels.SetEquals(ReceivedFromInputChannels);

            // TODO: Add input buffers if implementing input blocking/unblocking
            // public Dictionary<string, Queue<object>> InputBuffers { get; } = new();

            public BarrierState(long checkpointId)
            {
                CheckpointId = checkpointId;
            }

            public void RecordBarrierArrival(string inputChannelId, long timestamp)
            {
                if (ReceivedFromInputChannels.Count == 0) // First barrier for this checkpoint
                {
                    CheckpointTimestamp = timestamp;
                }
                ReceivedFromInputChannels.Add(inputChannelId);
            }

            public bool IsAligned(HashSet<string> expectedInputChannels)
            {
                return expectedInputChannels.SetEquals(ReceivedFromInputChannels);
            }
        }

        private readonly string _taskIdentifier; // e.g., "{jobVertexId}_{subtaskIndex}"
        private readonly HashSet<string> _expectedInputChannels;
        private readonly ConcurrentDictionary<long, BarrierState> _checkpointStates = new();

        // Action to perform when a barrier is aligned (e.g., trigger snapshot and forward barrier)
        private readonly Func<CheckpointBarrier, Task> _onBarrierAlignedAndSnapshotTrigger;

        public OperatorBarrierHandler(
            string jobVertexId,
            int subtaskIndex,
            List<string> expectedInputChannelIds, // List of "{sourceJobVertexId}_{sourceSubtaskIndex}"
            Func<CheckpointBarrier, Task> onBarrierAlignedAndSnapshotTrigger)
        {
            _taskIdentifier = $"{jobVertexId}_{subtaskIndex}";
            _expectedInputChannels = new HashSet<string>(expectedInputChannelIds);
            _onBarrierAlignedAndSnapshotTrigger = onBarrierAlignedAndSnapshotTrigger;

            if (!_expectedInputChannels.Any())
            {
                Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: Initialized with NO expected input channels. Barrier alignment will be trivial (or skipped if no barriers arrive).");
            }
            else
            {
                Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: Initialized. Expecting inputs from: {string.Join(", ", _expectedInputChannels)}");
            }
        }

        public async Task HandleIncomingBarrier(long checkpointId, long timestamp, string inputChannelId)
        {
            Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: Received barrier for CP {checkpointId} from input {inputChannelId}.");

            if (!_expectedInputChannels.Contains(inputChannelId) && _expectedInputChannels.Any())
            {
                Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: WARNING - Received barrier for CP {checkpointId} from unexpected input channel {inputChannelId}. Expected: [{string.Join(", ", _expectedInputChannels)}]. Ignoring for alignment purposes of this operator, but this might indicate a wiring issue.");
                return;
            }

            var state = _checkpointStates.GetOrAdd(checkpointId, (_) => new BarrierState(checkpointId));

            bool shouldTrigger = false;
            CheckpointBarrier? barrierToForward = null;

            lock (state) // Ensure thread-safe updates to BarrierState
            {
                if (state.IsAligned(_expectedInputChannels)) // Pass _expectedInputChannels here
                {
                    Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: CP {checkpointId} is already aligned. Ignoring duplicate barrier from {inputChannelId}.");
                    return; // Already aligned and processed
                }

                state.RecordBarrierArrival(inputChannelId, timestamp);
                Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: CP {checkpointId} progress: {state.ReceivedFromInputChannels.Count}/{_expectedInputChannels.Count} inputs received barrier.");

                bool isNowAligned = state.IsAligned(_expectedInputChannels);
                bool noInputsButBarrierReceived = _expectedInputChannels.Count == 0 && state.ReceivedFromInputChannels.Count > 0;

                if (isNowAligned || noInputsButBarrierReceived)
                {
                    if(noInputsButBarrierReceived)
                    {
                        Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: CP {checkpointId} has no declared inputs but received a barrier. Treating as aligned.");
                    }
                    Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: CP {checkpointId} is NOW ALIGNED.");
                    barrierToForward = new CheckpointBarrier(state.CheckpointId, state.CheckpointTimestamp);
                    shouldTrigger = true; // Mark that we need to trigger outside the lock if possible, or make callback aware
                }
            }

            if (shouldTrigger && barrierToForward != null)
            {
                // Schedule the callback, but don't block the HandleIncomingBarrier method for too long.
                // The callback will handle snapshotting and forwarding.
                _ = Task.Run(async () => {
                    try
                    {
                        await _onBarrierAlignedAndSnapshotTrigger(barrierToForward);
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: Exception during OnBarrierAlignedAndSnapshotTrigger for CP {checkpointId}: {ex}");
                    }
                    finally
                    {
                        // Clean up state for this checkpoint ID after processing
                        _checkpointStates.TryRemove(checkpointId, out _);
                        Console.WriteLine($"OperatorBarrierHandler [{_taskIdentifier}]: Cleaned up state for CP {checkpointId}.");
                    }
                });
            }
        }

        // TODO: Add methods for handling data records if input buffering is implemented.
        // public bool ShouldBuffer(string inputChannelId, long currentCheckpointId) { ... }
        // public void BufferRecord(string inputChannelId, object record) { ... }
        // public IEnumerable<object> GetUnbufferedRecords(string inputChannelId) { ... }
    }
}
#nullable disable
