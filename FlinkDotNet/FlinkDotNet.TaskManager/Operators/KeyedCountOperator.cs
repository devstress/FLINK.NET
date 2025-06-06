#nullable enable
using System;
using System.Collections.Generic;
using System.Linq; // Required for .Any() and .ToDictionary()
using System.Text.Json; // For JsonSerializer
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.States;
using FlinkDotNet.Core.Abstractions.Storage; // For SnapshotHandle
using FlinkDotNet.Storage.FileSystem; // For FileSystemSnapshotStore

namespace FlinkDotNet.TaskManager.Operators
{
    public class KeyedCountOperator : IMapOperator<string, string>, IOperatorLifecycle, ICheckpointableOperator
    {
        private string _operatorName = nameof(KeyedCountOperator);
        private IRuntimeContext? _runtimeContext;
        private IValueState<long>? _countState;

        private const string CountStateName = "keyedCounter";

        public void Open(IRuntimeContext context)
        {
            _runtimeContext = context;
            _operatorName = $"{context.TaskName}_{context.IndexOfThisSubtask}"; // More unique name

            var descriptor = new ValueStateDescriptor<long>(CountStateName, 0L);
            _countState = _runtimeContext.GetValueState(descriptor);

            Console.WriteLine($"[{_operatorName}] KeyedCountOperator opened. State '{CountStateName}' initialized.");
        }

        public string Map(string inputRecord) // Assuming inputRecord is the data, key is set on _runtimeContext
        {
            if (_runtimeContext == null || _countState == null)
            {
                throw new InvalidOperationException("Operator not opened or state not initialized.");
            }

            object? currentKey = _runtimeContext.GetCurrentKey();
            if (currentKey == null)
            {
                // This could happen if TaskExecutor didn't set the key,
                // or if this operator is used in a non-keyed context mistakenly.
                Console.WriteLine($"[{_operatorName}] WARNING: Current key is null. Processing record '{inputRecord}' without key scope for count state (will use a placeholder key or fail).");
                // For this example, let's throw if key is expected.
                // If this operator *could* run in non-keyed too, it would need different logic.
                throw new InvalidOperationException("KeyedCountOperator expects a current key to be set in the runtime context.");
            }

            long currentCount = _countState.Value(); // Gets value for currentKey
            long newCount = currentCount + 1;
            _countState.Update(newCount); // Updates value for currentKey

            string output = $"Key '{currentKey}' (input: '{inputRecord}') count is {newCount}";
            // Console.WriteLine($"[{_operatorName}] {output}");
            return output;
        }

        public async Task<OperatorStateSnapshot> SnapshotState(long checkpointId, long checkpointTimestamp)
        {
            if (_runtimeContext == null || _countState == null)
            {
                throw new InvalidOperationException("Operator not opened or state not initialized for snapshot.");
            }

            string operatorInstanceId = _runtimeContext.TaskName; // e.g., "MyJob_KeyedCounter_0"
            Console.WriteLine($"[{_operatorName}] SnapshotState called for CP ID: {checkpointId}, OperatorInstanceId: {operatorInstanceId}");

            var aggregatedKeyedStates = new Dictionary<string, object>();

            // Directly use the interface method. The null-forgiving operator ! assumes _countState is initialized.
            var counterStateData = _countState!.GetSerializedStateEntries();
            aggregatedKeyedStates.Add(CountStateName, counterStateData);
            Console.WriteLine($"[{_operatorName}] Got {counterStateData.Count} entries from '{CountStateName}' for snapshot CP {checkpointId}.");

            string statePayloadString;
            byte[] statePayloadBytes;

            if (aggregatedKeyedStates.Any())
            {
                 statePayloadString = System.Text.Json.JsonSerializer.Serialize(aggregatedKeyedStates, new JsonSerializerOptions { WriteIndented = true });
                 statePayloadBytes = System.Text.Encoding.UTF8.GetBytes(statePayloadString);
            }
            else
            {
                statePayloadString = "No keyed state to snapshot.";
                statePayloadBytes = System.Text.Encoding.UTF8.GetBytes(statePayloadString);
            }
            Console.WriteLine($"[{_operatorName}] Serialized state payload for CP {checkpointId}: {statePayloadString}");

            var snapshotStore = new FileSystemSnapshotStore();
            string currentTaskManagerId = Program.TaskManagerId;
            string jobGraphJobId = _runtimeContext.JobName;

            var snapshotHandleRecord = await snapshotStore.StoreSnapshot(
                jobGraphJobId,
                checkpointId,
                currentTaskManagerId,
                operatorInstanceId,
                statePayloadBytes);

            Console.WriteLine($"[{_operatorName}] Stored aggregated state for CP {checkpointId}. Handle: {snapshotHandleRecord.Value}, Size: {statePayloadBytes.Length}");

            var snapshot = new OperatorStateSnapshot(snapshotHandleRecord.Value, statePayloadBytes.Length);
            snapshot.Metadata = new Dictionary<string, string>
            {
                { "OperatorType", nameof(KeyedCountOperator) },
                { "OperatorInstanceId", operatorInstanceId },
                { "CheckpointTime", DateTime.UtcNow.ToString("o") },
                { "TaskManagerId", currentTaskManagerId },
                { "StateType", "AggregatedKeyedStates_Json" },
                { "OriginalPayloadPreview", statePayloadString.Substring(0, Math.Min(statePayloadString.Length, 100)) }
            };
            return snapshot;
        }

        public async Task RestoreState(OperatorStateSnapshot snapshotDetails)
        {
            if (_runtimeContext == null || _countState == null)
            {
                throw new InvalidOperationException("Operator not opened or state not initialized for restore.");
            }

            string operatorInstanceId = _runtimeContext.TaskName;
            Console.WriteLine($"[{_operatorName}] RestoreState called for OperatorInstanceId: {operatorInstanceId}. Handle: {snapshotDetails.StateHandle}");

            if (string.IsNullOrEmpty(snapshotDetails.StateHandle))
            {
                Console.WriteLine($"[{_operatorName}] RestoreState: Snapshot handle is null or empty. No state to restore.");
                return;
            }

            var snapshotStore = new FileSystemSnapshotStore();
            byte[]? statePayloadBytes = null;
            try
            {
                var snapshotHandleInstance = new SnapshotHandle(snapshotDetails.StateHandle);
                statePayloadBytes = await snapshotStore.RetrieveSnapshot(snapshotHandleInstance);
                if (statePayloadBytes == null)
                {
                     Console.WriteLine($"[{_operatorName}] RestoreState: Snapshot data not found by store for handle: {snapshotDetails.StateHandle}");
                     return;
                }
            }
            catch (ArgumentException argEx)
            {
                Console.WriteLine($"[{_operatorName}] RestoreState: Invalid state handle. Ex: {argEx.Message}");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_operatorName}] RestoreState: Error reading snapshot data using store for handle {snapshotDetails.StateHandle}. Ex: {ex.Message}");
                return;
            }

            if (statePayloadBytes.Length == 0) // Check directly if length is 0, as RetrieveSnapshot returns null if not found.
            {
                Console.WriteLine($"[{_operatorName}] RestoreState: Snapshot data is empty for handle {snapshotDetails.StateHandle}.");
                return;
            }

            var aggregatedKeyedStates = JsonSerializer.Deserialize<Dictionary<string, object>>(statePayloadBytes);

            if (aggregatedKeyedStates != null && aggregatedKeyedStates.TryGetValue(CountStateName, out object? stateDataObj))
            {
                Dictionary<object, long>? counterStateData = null;
                if (stateDataObj is JsonElement jsonElement)
                {
                    try
                    {
                        // Attempt to deserialize as Dictionary<string, long> first, as JSON keys are typically strings
                        var tempDictStringKey = JsonSerializer.Deserialize<Dictionary<string, long>>(jsonElement.GetRawText());
                        if (tempDictStringKey != null)
                        {
                             counterStateData = tempDictStringKey.ToDictionary(kvp => (object)kvp.Key, kvp => kvp.Value);
                        }
                    } catch (JsonException) {
                        // Fallback: try to deserialize as Dictionary<object, long> if keys were numbers etc.
                        // This path is less likely to work directly with System.Text.Json if keys aren't strings.
                        // For robust solution, keys might need to be consistently stored as strings in JSON if using object key type.
                        Console.WriteLine($"[{_operatorName}] RestoreState: Could not deserialize '{CountStateName}' state as Dictionary<string,long>. Attempting Dictionary<object,long> (may require custom converter for object keys).");
                        try {
                            counterStateData = JsonSerializer.Deserialize<Dictionary<object, long>>(jsonElement.GetRawText());
                        } catch (JsonException exInner) {
                             Console.WriteLine($"[{_operatorName}] RestoreState: Failed to deserialize '{CountStateName}' state data as Dictionary<object,long>. JsonElement content: {jsonElement.GetRawText()}. Error: {exInner.Message}");
                        }
                    }
                }

                if (counterStateData != null)
                {
                    _countState!.RestoreSerializedStateEntries(counterStateData); // Use interface method
                    Console.WriteLine($"[{_operatorName}] Restored {counterStateData.Count} entries into '{CountStateName}'.");
                }
                else
                {
                     Console.WriteLine($"[{_operatorName}] RestoreState: Failed to deserialize state data for '{CountStateName}'.");
                }
            }
            else
            {
                 Console.WriteLine($"[{_operatorName}] RestoreState: State for '{CountStateName}' not found in snapshot or deserialized aggregate is null.");
            }
        }

        public void Close()
        {
             Console.WriteLine($"[{_operatorName}] KeyedCountOperator closed.");
        }
    }
}
#nullable disable
