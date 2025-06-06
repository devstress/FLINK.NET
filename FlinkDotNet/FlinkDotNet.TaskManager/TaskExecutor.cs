#nullable enable
using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Operators; // For IMapOperator
using FlinkDotNet.Core.Abstractions.Runtime;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Serializers; // For ITypeSerializer
using FlinkDotNet.Core.Abstractions.Models.Checkpointing; // Added for CheckpointBarrier
using FlinkDotNet.Core.Abstractions.Models.State; // Added for OperatorStateSnapshot
using FlinkDotNet.Core.Abstractions.Operators; // Added for ICheckpointableOperator
using FlinkDotNet.Storage.FileSystem; // Added for FileSystemSnapshotStore
using System.Text; // Added for Encoding
using FlinkDotNet.Core.Abstractions.Execution; // For SerializerRegistry
using FlinkDotNet.Proto.Internal;
using Grpc.Net.Client;
using Grpc.Core;
using Google.Protobuf;
using FlinkDotNet.TaskManager.Services;
using System.Collections.Generic;
using System.Collections.Concurrent; // Added for SourceTaskWrapper if needed elsewhere, good to have for registry interactions
using System.Diagnostics.Metrics; // Added for Metrics
using System.Globalization; // For CultureInfo
using System.Threading.Channels; // Added for SourceTaskWrapper and barrier injection


// May need to add 'using' for ConsoleSinkFunction and FileSourceFunction if used directly by type name
// For dynamic loading, their projects need to be referenced by TaskManager or assemblies loaded.

namespace FlinkDotNet.TaskManager
{
    internal static class TaskManagerMetrics
    {
        private static readonly Meter TaskExecutorMeter = new Meter("FlinkDotNet.TaskManager.TaskExecutor", "1.0.0");
        internal static readonly Counter<long> RecordsSent = TaskExecutorMeter.CreateCounter<long>("flinkdotnet.taskmanager.records_sent", unit: "{records}", description: "Number of records sent by NetworkedCollector.");
    }

    // LOGGING_PLACEHOLDER:
    // private readonly Microsoft.Extensions.Logging.ILogger<TaskExecutor> _logger; // Inject via constructor, ensure using Microsoft.Extensions.Logging;

    // Basic context implementations for this PoC
    public class SimpleSourceContext<T> : ISourceContext<T>
    {
        private readonly Action<T> _collectAction;
        public SimpleSourceContext(Action<T> collectAction) => _collectAction = collectAction;
        public void Collect(T record) => _collectAction(record);
    }

    public class SimpleSinkContext : ISinkContext
    {
        public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public class TaskExecutor
    {
        private readonly ActiveTaskRegistry _activeTaskRegistry;
        private readonly TaskManagerCheckpointingServiceImpl _checkpointingService;
        private readonly SerializerRegistry _serializerRegistry;
        private readonly ConcurrentDictionary<string, OperatorBarrierHandler> _operatorBarrierHandlers = new();

        public TaskExecutor(
            ActiveTaskRegistry activeTaskRegistry,
            TaskManagerCheckpointingServiceImpl checkpointingService,
            SerializerRegistry serializerRegistry)
        {
            _activeTaskRegistry = activeTaskRegistry;
            _checkpointingService = checkpointingService;
            _serializerRegistry = serializerRegistry;
        }

        public OperatorBarrierHandler? GetOperatorBarrierHandler(string jobVertexId, int subtaskIndex)
        {
            if (_operatorBarrierHandlers.TryGetValue($"{jobVertexId}_{subtaskIndex}", out var handler))
            {
                return handler;
            }
            return null;
        }

        // SourceTaskWrapper class definition
        private class SourceTaskWrapper : IBarrierInjectableSource
        {
            public string JobId { get; }
            public string JobVertexId { get; }
            public int SubtaskIndex { get; }
            public ChannelWriter<BarrierInjectionRequest> BarrierChannelWriter { get; }
            private readonly Action _onCompleted;
            public ChannelReader<BarrierInjectionRequest> BarrierChannelReader { get; }

            public SourceTaskWrapper(string jobId, string jobVertexId, int subtaskIndex, Action onCompleted)
            {
                JobId = jobId;
                JobVertexId = jobVertexId;
                SubtaskIndex = subtaskIndex;
                _onCompleted = onCompleted;

                var channel = System.Threading.Channels.Channel.CreateUnbounded<BarrierInjectionRequest>();
                BarrierChannelWriter = channel.Writer;
                BarrierChannelReader = channel.Reader;
            }

            public void ReportCompleted()
            {
                BarrierChannelWriter.TryComplete();
                _onCompleted.Invoke();
            }
        }

        // NetworkedCollector class definition from prompt
        public class NetworkedCollector<T, TKey> : INetworkedCollector // T is ElementType, TKey is KeyType
        {
            private readonly string _sourceJobVertexId;
            private readonly int _sourceSubtaskIndex;
            private readonly OperatorOutput _outputInfo; // Changed from TaskDeploymentDescriptor.Types.OperatorOutput
            private readonly ITypeSerializer<T> _serializer;
            private readonly string _targetTmEndpoint;
            private DataExchangeService.DataExchangeServiceClient? _client;
            private AsyncClientStreamingCall<DataRecord, DataAck>? _streamCall;

            private readonly Abstractions.Functions.IKeySelector<T, TKey>? _keySelector;
            private readonly int? _downstreamParallelism;

            private const int MaxOutstandingSends = 100; // Or make it configurable
            private readonly SemaphoreSlim _sendPermits;

            public NetworkedCollector(
                string sourceJobVertexId,
                int sourceSubtaskIndex,
                OperatorOutput outputInfo, // from TDD.Outputs
                ITypeSerializer<T> serializer,
                Abstractions.Functions.IKeySelector<T, TKey>? keySelector, // New parameter
                int? downstreamParallelism) // New parameter
            {
                _sendPermits = new SemaphoreSlim(MaxOutstandingSends, MaxOutstandingSends); // Initialize semaphore
                _sourceJobVertexId = sourceJobVertexId;
                _sourceSubtaskIndex = sourceSubtaskIndex;
                _outputInfo = outputInfo;
                _serializer = serializer;
                _keySelector = keySelector;
                _downstreamParallelism = downstreamParallelism;
                _targetTmEndpoint = _outputInfo.TargetTaskEndpoint; // Get from outputInfo
                Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] NetworkedCollector created for target {_outputInfo.TargetVertexId} at {_targetTmEndpoint}. Keyed: {_keySelector != null}");
            }

            public string TargetVertexId => _outputInfo.TargetVertexId;

            public async Task CollectObject(object record, CancellationToken cancellationToken)
            {
                // This assumes that for any collector that needs to handle barriers, T is 'object'.
                // The Collect method's "if (record is CheckpointBarrier actualBarrier)" will then work correctly.
                await Collect((T)record, cancellationToken);
            }

            private async Task EnsureStreamOpenAsync(CancellationToken cancellationToken)
            {
                if (_streamCall == null)
                {
                    if (string.IsNullOrEmpty(_targetTmEndpoint) || _targetTmEndpoint.Contains(":0")) // Port 0 is invalid
                    {
                        Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] ERROR: Invalid target TM endpoint '{_targetTmEndpoint}' for target vertex {_outputInfo.TargetVertexId}. Cannot open stream.");
                        return;
                    }
                    try
                    {
                        var channel = GrpcChannel.ForAddress(_targetTmEndpoint);
                        _client = new DataExchangeService.DataExchangeServiceClient(channel);
                        _streamCall = _client.SendData(cancellationToken: cancellationToken);
                        Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] Opened SendData stream to {_targetTmEndpoint} for target {_outputInfo.TargetVertexId}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] ERROR: Failed to open SendData stream to {_targetTmEndpoint} for target vertex {_outputInfo.TargetVertexId}: {ex.Message}");
                        _streamCall = null; // Ensure it's null if opening failed
                    }
                }
            }

            public async Task Collect(T record, CancellationToken cancellationToken) // Assuming T is object here, or string
            {
                await _sendPermits.WaitAsync(cancellationToken); // Existing throttling

                try
                {
                    await EnsureStreamOpenAsync(cancellationToken);
                    if (_streamCall == null)
                    {
                        Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] ERROR: Stream call is null, cannot send record/barrier to {_outputInfo.TargetVertexId}.");
                        // _sendPermits.Release(); // This is handled by finally
                        return;
                    }

                    DataRecord dataRecordToSend;

                    if (record is FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier actualBarrier)
                    {
                        Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] NetworkedCollector received CheckpointBarrier object: ID {actualBarrier.CheckpointId}");
                        var protoBarrier = new FlinkDotNet.Proto.Internal.CheckpointBarrier // Ensure this is the correct namespace for the proto generated class
                        {
                            CheckpointId = actualBarrier.CheckpointId,
                            CheckpointTimestamp = actualBarrier.Timestamp
                        };

                        dataRecordToSend = new DataRecord
                        {
                            TargetJobVertexId = _outputInfo.TargetVertexId,
                            TargetSubtaskIndex = _outputInfo.TargetSpecificSubtaskIndex,
                            SourceJobVertexId = _sourceJobVertexId, // Populate source info
                            SourceSubtaskIndex = _sourceSubtaskIndex, // Populate source info
                            IsCheckpointBarrier = true,
                            BarrierPayload = protoBarrier, // Assign to the oneof field
                            // DataPayload should not be set, or explicitly set to ByteString.Empty if the oneof semantics require it
                        };
                         Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] Prepared Protobuf Barrier ID: {protoBarrier.CheckpointId}");
                    }
                    else
                    {
                        // Regular data record
                        var payloadBytes = _serializer.Serialize(record);
                        dataRecordToSend = new DataRecord
                        {
                            TargetJobVertexId = _outputInfo.TargetVertexId,
                            TargetSubtaskIndex = _outputInfo.TargetSpecificSubtaskIndex,
                            SourceJobVertexId = _sourceJobVertexId, // Populate source info
                            SourceSubtaskIndex = _sourceSubtaskIndex, // Populate source info
                            IsCheckpointBarrier = false,
                            DataPayload = ByteString.CopyFrom(payloadBytes) // Assign to the oneof field for data
                            // BarrierPayload should not be set
                        };
                    }

                    if (_keySelector != null && _downstreamParallelism.HasValue && _downstreamParallelism.Value > 0 && !dataRecordToSend.IsCheckpointBarrier) // Keying logic only for actual data
                    {
                        // This assumes 'record' is the original T input to Collect.
                        // If T is object (e.g. for CheckpointBarrier), this GetKey call would need careful handling
                        // or 'record' would need to be cast to the actual element type T before calling GetKey.
                        // For now, assuming 'record' is of the correct type T for the _keySelector.
                        TKey key = _keySelector.GetKey(record);
                        int hashCode = key == null ? 0 : key.GetHashCode();
                        int targetSubtask = (hashCode & int.MaxValue) % _downstreamParallelism.Value;
                        dataRecordToSend.TargetSubtaskIndex = targetSubtask;
                        // Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] Keyed routing for record. Key hash: {hashCode}, Target subtask: {targetSubtask} (of {_downstreamParallelism.Value}) for target vertex {_outputInfo.TargetVertexId}");
                    }
                    else
                    {
                        // For barriers or non-keyed outputs, use the pre-configured target subtask index from OperatorOutput
                        dataRecordToSend.TargetSubtaskIndex = _outputInfo.TargetSpecificSubtaskIndex;
                    }

                    await _streamCall.RequestStream.WriteAsync(dataRecordToSend);

                    if (!dataRecordToSend.IsCheckpointBarrier)
                    {
                        TaskManagerMetrics.RecordsSent.Add(1); // Increment only for actual data records
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] Collect operation cancelled for {_outputInfo.TargetVertexId}.");
                    // Permit is released in finally. If WaitAsync itself was cancelled, it wouldn't have acquired the permit.
                    // If cancellation happened after WaitAsync, finally will release it.
                    // throw; // Re-throw if cancellation needs to propagate and be handled by caller.
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] Error sending data/barrier to {_outputInfo.TargetVertexId}: {ex.Message}. Closing stream.");
                    await CloseStreamAsync(); // Close stream on error
                    // Permit is released in finally
                }
                finally
                {
                    _sendPermits.Release(); // Release permit in all cases where WaitAsync completed successfully
                }
            }

            public async Task CloseStreamAsync()
            {
                if (_streamCall != null)
                {
                    try
                    {
                        await _streamCall.RequestStream.CompleteAsync();
                        await _streamCall.ResponseAsync;
                        Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] SendData stream to {_targetTmEndpoint} for target {_outputInfo.TargetVertexId} completed.");
                    }
                    catch (Exception ex) { Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] Error closing stream to {_outputInfo.TargetVertexId}: {ex.Message}"); }
                    _streamCall.Dispose();
                    _streamCall = null;
                }
            }
        }


        // Old ExecuteTask method removed.

        // New method from prompt
        public async Task ExecuteFromDescriptor(
            TaskDeploymentDescriptor tdd,
            Dictionary<string, string> operatorProperties, // Already deserialized from TDD
        CancellationToken cancellationToken)
    {
        // METRICS_PLACEHOLDER:
        // string taskInstanceId = $"{tdd.JobVertexId}_{tdd.SubtaskIndex}";
        // var taskMetrics = new FlinkDotNet.TaskManager.Models.TaskMetrics { TaskId = taskInstanceId };
        // TaskMetricsRegistry.Register(taskInstanceId, taskMetrics); // Assuming a static registry or instance member like:
        //                                                              // public static ConcurrentDictionary<string, FlinkDotNet.TaskManager.Models.TaskMetrics> AllTaskMetrics = new();
        // cancellationToken.Register(() => TaskMetricsRegistry.Unregister(taskInstanceId));

        Console.WriteLine($"[{tdd.TaskName}] Attempting to execute task from TDD. Operator: {tdd.FullyQualifiedOperatorName}");
        IRuntimeContext runtimeContext = new BasicRuntimeContext(
            jobName: tdd.JobGraphJobId,
            taskName: tdd.TaskName,
            numberOfParallelSubtasks: 0, // TODO: Get total parallelism for this vertex from TDD or other source
            indexOfThisSubtask: tdd.SubtaskIndex
        );
        // (runtimeContext as BasicRuntimeContext)?.SetCurrentKey(...); // If needed and key is available

        object? sourceInstance = null;
        IMapOperator<object, object>? operatorInstance = null;
        ISinkFunction<object>? sinkInstance = null;

        // TODO: This list will need to store collectors of different generic types or use a common non-generic interface.
        // For this subtask, we are focusing on NetworkedCollector's internal changes.
        // The instantiation logic below for NetworkedCollector<object> will be incorrect once NetworkedCollector is fully generic.
        // This will be addressed in a subsequent subtask.
        var collectors = new List<INetworkedCollector>(); // Changed to List<INetworkedCollector>

        bool isSource = tdd.Inputs.Count == 0 && tdd.Outputs.Count > 0;
        bool isSink = tdd.Inputs.Count > 0 && tdd.Outputs.Count == 0;
        bool isOperator = tdd.Inputs.Count > 0 && tdd.Outputs.Count > 0;

        Type? componentType = Type.GetType(tdd.FullyQualifiedOperatorName);
        if (componentType == null)
        {
            Console.WriteLine($"[{tdd.TaskName}] ERROR: Component type '{tdd.FullyQualifiedOperatorName}' not found.");
            await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync())); // Close any open streams
            return;
        }

        // For Source/Operator: Setup output serializer and collectors
        ITypeSerializer<object>? outputDataSerializer = null;
        if (isSource || isOperator)
        {
            if (string.IsNullOrEmpty(tdd.OutputSerializerTypeName) || string.IsNullOrEmpty(tdd.OutputTypeName))
            {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: Output type or serializer not defined for a non-sink vertex.");
                 await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync()));
                 return;
            }
            Type? outSerType = Type.GetType(tdd.OutputSerializerTypeName);
            // This PoC assumes TDD.OutputTypeName matches T of ITypeSerializer<T> for OutputDataSerializer
            // And NetworkedCollector<object> will use ITypeSerializer<object>.
            // outputDataSerializer = Activator.CreateInstance(outSerType) as ITypeSerializer<object>; // This was for ITypeSerializer<object>
            // We will get specific serializers inside the loop now.
            // else { Console.WriteLine($"[{tdd.TaskName}] WARNING: Output serializer type '{tdd.OutputSerializerTypeName}' not found."); }

            // if (outputDataSerializer == null) { // This check might be too early now
            //      Console.WriteLine($"[{tdd.TaskName}] ERROR: Output serializer could not be instantiated for outputs.");
            //      await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync())); // Close collectors if any were made
            //      return;
            // }

            foreach (var outputDesc in tdd.Outputs) // Use outputDesc from TDD
            {
                Type? actualOutputType = null;
                if (!string.IsNullOrEmpty(tdd.OutputTypeName))
                {
                    actualOutputType = Type.GetType(tdd.OutputTypeName);
                }
                else if (isSource)
                {
                    Console.WriteLine($"[{tdd.TaskName}] ERROR: Source task missing OutputTypeName for output to {outputDesc.TargetVertexId}.");
                    continue;
                }

                if (actualOutputType == null)
                {
                    Console.WriteLine($"[{tdd.TaskName}] ERROR: Could not determine actual output type for collector for target {outputDesc.TargetVertexId}. TDD.OutputTypeName: {tdd.OutputTypeName}");
                    continue;
                }

                ITypeSerializer specificSerializerForCollector = _serializerRegistry.GetSerializer(actualOutputType);

                var keyingInfo = tdd.OutputKeyingInfo.FirstOrDefault(k => k.TargetJobVertexId == outputDesc.TargetVertexId);

                if (keyingInfo != null) // Keyed Output
                {
                    Type? keySelectorActualType = Type.GetType(keyingInfo.KeySelectorTypeName);
                    Type? keyActualType = Type.GetType(keyingInfo.KeyTypeName);

                    if (keySelectorActualType == null || keyActualType == null)
                    {
                        Console.WriteLine($"[{tdd.TaskName}] ERROR: Could not load KeySelectorType ('{keyingInfo.KeySelectorTypeName}') or KeyType ('{keyingInfo.KeyTypeName}') for keyed output to {outputDesc.TargetVertexId}.");
                        continue;
                    }

                    object? keySelectorInstance = null;
                    try
                    {
                        byte[] serializedKeySelectorBytes = Convert.FromBase64String(keyingInfo.SerializedKeySelector);
                        ITypeSerializer keySelectorSerializer = _serializerRegistry.GetSerializer(keySelectorActualType);
                        keySelectorInstance = keySelectorSerializer.Deserialize(serializedKeySelectorBytes);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to deserialize KeySelector for output to {outputDesc.TargetVertexId}. Type: {keySelectorActualType.FullName}. Ex: {ex.Message}");
                        continue;
                    }

                    if (keySelectorInstance == null)
                    {
                        Console.WriteLine($"[{tdd.TaskName}] ERROR: Deserialized KeySelector is null for output to {outputDesc.TargetVertexId}.");
                        continue;
                    }

                    Type genericCollectorType = typeof(NetworkedCollector<,>);
                    Type specificCollectorType = genericCollectorType.MakeGenericType(actualOutputType, keyActualType);

                    INetworkedCollector collector = (INetworkedCollector)Activator.CreateInstance(
                        specificCollectorType,
                        tdd.JobVertexId,
                        tdd.SubtaskIndex,
                        outputDesc,
                        specificSerializerForCollector, // ITypeSerializer<ActualOutputType>
                        keySelectorInstance, // IKeySelector<ActualOutputType, KeyActualType>
                        keyingInfo.DownstreamParallelism
                    )!;
                    collectors.Add(collector);
                    Console.WriteLine($"[{tdd.TaskName}] Added KEYED NetworkedCollector for target {outputDesc.TargetVertexId}. ElementType: {actualOutputType.Name}, KeyType: {keyActualType.Name}");
                }
                else // Non-Keyed Output
                {
                    Type genericCollectorType = typeof(NetworkedCollector<,>);
                    Type specificCollectorType = genericCollectorType.MakeGenericType(actualOutputType, typeof(object)); // TKey is dummy typeof(object)

                    INetworkedCollector collector = (INetworkedCollector)Activator.CreateInstance(
                        specificCollectorType,
                        tdd.JobVertexId,
                        tdd.SubtaskIndex,
                        outputDesc,
                        specificSerializerForCollector, // ITypeSerializer<ActualOutputType>
                        null, // no key selector
                        null  // no parallelism
                    )!;
                    collectors.Add(collector);
                    Console.WriteLine($"[{tdd.TaskName}] Added NON-KEYED NetworkedCollector for target {outputDesc.TargetVertexId}. ElementType: {actualOutputType.Name}");
                }
            }
        }

        // For Operator/Sink: Setup input serializer and register receiver
        ITypeSerializer<object>? inputDataSerializer = null;
        if (isOperator || isSink)
        {
            if (string.IsNullOrEmpty(tdd.InputSerializerTypeName) || string.IsNullOrEmpty(tdd.InputTypeName))
            {
                Console.WriteLine($"[{tdd.TaskName}] ERROR: Input type or serializer not defined for a non-source vertex.");
                await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync()));
                return;
            }
            Type? inSerType = Type.GetType(tdd.InputSerializerTypeName);
            if (inSerType != null) inputDataSerializer = Activator.CreateInstance(inSerType) as ITypeSerializer<object>;
            else { Console.WriteLine($"[{tdd.TaskName}] WARNING: Input serializer type '{tdd.InputSerializerTypeName}' not found.");}

            if (inputDataSerializer == null) {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: Input serializer could not be instantiated.");
                 await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync()));
                 return;
            }
        }

        // Instantiate the core component (Source, Operator, or Sink)
        if (isSource)
        {
            // Example: FileSourceFunction<string>
            // This logic needs to be robust for generic types based on tdd.OutputTypeName
            if (tdd.FullyQualifiedOperatorName.StartsWith("FlinkDotNet.Connectors.Sources.File.FileSourceFunction`1") &&
                tdd.OutputTypeName == "System.String")
            {
                Type genericType = componentType.MakeGenericType(typeof(string));
                // FileSourceFunction constructor expects ITypeSerializer<TOut> where TOut is string here.
                // outputDataSerializer is ITypeSerializer<object>. This is a mismatch if not handled.
                // For PoC, we assume StringSerializer is used, and it's castable or wrapped.
                var stringSerializerForSource = Activator.CreateInstance(Type.GetType(tdd.OutputSerializerTypeName!)!) as ITypeSerializer<string>;
                if (stringSerializerForSource == null) {
                     Console.WriteLine($"[{tdd.TaskName}] ERROR: StringSerializer for FileSourceFunction<string> not loaded.");
                     await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync()));
                     return;
                }
                sourceInstance = Activator.CreateInstance(genericType, operatorProperties["filePath"], stringSerializerForSource);
            }
        }
        else if (isOperator)
        {
             if (tdd.FullyQualifiedOperatorName.Contains("SimpleStringToUpperMapOperator") &&
                tdd.InputTypeName == "System.String" && tdd.OutputTypeName == "System.String")
            {
                operatorInstance = Activator.CreateInstance(componentType) as IMapOperator<object, object>;
                (operatorInstance as IOperatorLifecycle)?.Open(runtimeContext);
            }
        }
        else if (isSink)
        {
            if (tdd.FullyQualifiedOperatorName.StartsWith("FlinkDotNet.Connectors.Sinks.Console.ConsoleSinkFunction`1") &&
                tdd.InputTypeName == "System.String")
            {
                Type genericType = componentType.MakeGenericType(typeof(string));
                sinkInstance = Activator.CreateInstance(genericType) as ISinkFunction<object>; // This cast is problematic if ConsoleSinkFunction<string>
                (sinkInstance as IOperatorLifecycle)?.Open(runtimeContext);
            }
        }

        if ((isSource && sourceInstance == null) || (isOperator && operatorInstance == null) || (isSink && sinkInstance == null))
        {
            Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to instantiate component {tdd.FullyQualifiedOperatorName}. Check type names, properties, and constructors.");
            await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync()));
            return;
        }

        Console.WriteLine($"[{tdd.TaskName}] Component {tdd.FullyQualifiedOperatorName} instantiated successfully.");

        // Attempt to restore state if in recovery mode
        if (tdd.IsRecovery)
        {
            Console.WriteLine($"[{tdd.TaskName}] Task is in RECOVERY mode for checkpoint ID: {tdd.RecoveryCheckpointId}. Attempting to restore state.");
            if (string.IsNullOrEmpty(tdd.RecoverySnapshotHandle))
            {
                Console.WriteLine($"[{tdd.TaskName}] RECOVERY WARNING: Task is in recovery mode but RecoverySnapshotHandle is missing. Cannot restore state.");
            }
            else
            {
                object? componentToRestore = null;
                if (isOperator) componentToRestore = operatorInstance;
                else if (isSink) componentToRestore = sinkInstance;
                else if (isSource) componentToRestore = sourceInstance;

                if (componentToRestore is ICheckpointableOperator checkpointableComponent)
                {
                    try
                    {
                        var recoverySnapshot = new OperatorStateSnapshot(tdd.RecoverySnapshotHandle, 0L);
                        Console.WriteLine($"[{tdd.TaskName}] Calling RestoreState on component with handle: {tdd.RecoverySnapshotHandle}");
                        await checkpointableComponent.RestoreState(recoverySnapshot);
                        Console.WriteLine($"[{tdd.TaskName}] RestoreState completed for checkpoint ID: {tdd.RecoveryCheckpointId}.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{tdd.TaskName}] ERROR during RestoreState for checkpoint ID: {tdd.RecoveryCheckpointId}. Handle: {tdd.RecoverySnapshotHandle}. Exception: {ex.Message}");
                        // Consider re-throwing or setting a task-failed status.
                    }
                }
                else
                {
                    Console.WriteLine($"[{tdd.TaskName}] RECOVERY WARNING: Task is in recovery mode with a snapshot handle, but the main component '{componentToRestore?.GetType().FullName}' is not ICheckpointableOperator. Cannot restore state.");
                }
            }
        }

        // Register receiver for operators and sinks (AFTER potential state restoration)
        if ((isOperator || isSink) && inputDataSerializer != null)
        {
            ProcessRecordDelegate recordProcessor = async (targetJobVertexId, targetSubtaskIndex, payload) =>
            {
                if (targetJobVertexId != tdd.JobVertexId || targetSubtaskIndex != tdd.SubtaskIndex) return;
                try
                {
                    var deserializedRecord = inputDataSerializer.Deserialize(payload);

                    object? currentKeyForState = null;
                    if (tdd.InputKeyingInfo != null && tdd.InputKeyingInfo.Any())
                    {
                        var inputKeyingConfig = tdd.InputKeyingInfo.First();

                        if (!string.IsNullOrEmpty(inputKeyingConfig.SerializedKeySelector) &&
                            !string.IsNullOrEmpty(inputKeyingConfig.KeySelectorTypeName) &&
                            !string.IsNullOrEmpty(inputKeyingConfig.KeyTypeName)) // KeyTypeName check is good, but key_selector_type_name is new
                        {
                            try
                            {
                                Type? keySelectorType = Type.GetType(inputKeyingConfig.KeySelectorTypeName); // Use new field
                                if (keySelectorType != null)
                                {
                                    byte[] serializedBytes = Convert.FromBase64String(inputKeyingConfig.SerializedKeySelector);
                                    ITypeSerializer keySelectorSerializer = _serializerRegistry.GetSerializer(keySelectorType);
                                    object keySelectorInstance = keySelectorSerializer.Deserialize(serializedBytes)!;

                                    MethodInfo? getKeyMethod = keySelectorType.GetMethod("GetKey");
                                    if (getKeyMethod != null)
                                    {
                                        currentKeyForState = getKeyMethod.Invoke(keySelectorInstance, new object[] { deserializedRecord! });
                                    }
                                    else
                                    {
                                        Console.WriteLine($"[{tdd.TaskName}] ERROR: Could not find GetKey method on deserialized KeySelector type {keySelectorType.FullName}. Keyed state will not be properly scoped.");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine($"[{tdd.TaskName}] ERROR: Could not load KeySelectorType '{inputKeyingConfig.KeySelectorTypeName}' for input keying. Keyed state will not be properly scoped.");
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to deserialize or use input KeySelector for state keying. Ex: {ex.Message}. Keyed state will not be properly scoped.");
                            }
                        }
                    }

                    if (runtimeContext is BasicRuntimeContext brc)
                    {
                        brc.SetCurrentKey(currentKeyForState);
                    }

                    if (isOperator && operatorInstance != null)
                    {
                        var result = operatorInstance.Map(deserializedRecord);
                        foreach (INetworkedCollector collector in collectors) { await collector.CollectObject(result, cancellationToken); }
                    }
                    else if (isSink && sinkInstance != null)
                    {
                        sinkInstance.Invoke(deserializedRecord, new SimpleSinkContext());
                    }

                    // if (runtimeContext is BasicRuntimeContext brcAfter) { brcAfter.SetCurrentKey(null); }
                }
                catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] Error processing received record: {ex.Message}"); }
            };
            DataReceiverRegistry.RegisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex, recordProcessor);
            cancellationToken.Register(() => DataReceiverRegistry.UnregisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex));
        }


        // --- Execution Logic ---
        if (isSource && sourceInstance != null)
        {
            Console.WriteLine($"[{tdd.TaskName}] Running as a SOURCE task.");
            var sourceTaskWrapper = new SourceTaskWrapper(
                tdd.JobGraphJobId,
                tdd.JobVertexId,
                tdd.SubtaskIndex,
                () => _activeTaskRegistry.UnregisterSource(tdd.JobGraphJobId, tdd.JobVertexId, tdd.SubtaskIndex)
            );
            _activeTaskRegistry.RegisterSource(sourceTaskWrapper);

            try
            {
                if (sourceInstance is ISourceFunction<string> typedStringSource && tdd.OutputTypeName == "System.String")
                {
                    await Task.Run(async () => await RunSourceWithBarrierInjection<string>(
                        typedStringSource,
                        collectors,
                        sourceTaskWrapper.BarrierChannelReader,
                        cancellationToken,
                        () => typedStringSource.Cancel()
                    ), cancellationToken);
                }
                else if (sourceInstance is ISourceFunction<object> objectSource)
                {
                    await Task.Run(async () => await RunSourceWithBarrierInjection<object>(
                        objectSource,
                        collectors,
                        sourceTaskWrapper.BarrierChannelReader,
                        cancellationToken,
                        () => objectSource.Cancel()
                    ), cancellationToken);
                }
                else
                {
                     Console.WriteLine($"[{tdd.TaskName}] Source instance type {sourceInstance?.GetType()} not directly runnable with current barrier injection logic.");
                }
            }
            catch (OperationCanceledException) { Console.WriteLine($"[{tdd.TaskName}] Source run canceled."); }
            catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] Error running source: {ex.Message} {ex.StackTrace}");}
            finally
            {
                // Ensure ReportCompleted is called when the source logic truly finishes or is cancelled.
                // The ReportCompleted call is primarily handled by the callback in SourceTaskWrapper upon unregistration.
                // However, if the task is cancelled before registration or if unregistration fails,
                // an explicit call here might be a safeguard.
                // For now, relying on the unregistration callback.
                // sourceTaskWrapper.ReportCompleted(); // This might be redundant if unregistration path always calls it.
            }
        }
        else if (isOperator && operatorInstance != null) // Ensure operatorInstance is not null
        {
            var expectedInputIds = tdd.Inputs
                .Select(inp => $"{inp.UpstreamJobVertexId}_{inp.UpstreamSubtaskIndex}")
                .ToList();

            Func<CheckpointBarrier, Task> onAlignedCallback = async (alignedBarrier) =>
            {
                Console.WriteLine($"[{tdd.TaskName}] ALIGNED BARRIER {alignedBarrier.CheckpointId}. Triggering snapshot and forwarding barrier.");

                OperatorStateSnapshot? snapshotResult = null;
                if (operatorInstance is ICheckpointableOperator checkpointableOperator)
                {
                    try
                    {
                        Console.WriteLine($"[{tdd.TaskName}] Operator is ICheckpointableOperator. Calling SnapshotState for CP {alignedBarrier.CheckpointId}.");
                        snapshotResult = await checkpointableOperator.SnapshotState(alignedBarrier.CheckpointId, alignedBarrier.Timestamp);
                        Console.WriteLine($"[{tdd.TaskName}] SnapshotState for CP {alignedBarrier.CheckpointId} returned. Handle: {snapshotResult?.StateHandle}, Size: {snapshotResult?.StateSize}");

                        if (snapshotResult != null)
                        {
                            // For simplicity of this subtask, let's use a placeholder duration.
                            long placeholderDurationMs = 50; // Replace with actual timing

                            // Report completion to TaskManagerCheckpointingServiceImpl
                            await _checkpointingService.ReportOperatorSnapshotComplete(
                                tdd.JobGraphJobId,
                                alignedBarrier.CheckpointId,
                                tdd.JobVertexId, // This is the JobVertexId of the operator task
                                tdd.SubtaskIndex, // Subtask index of the operator task
                                snapshotResult,
                                placeholderDurationMs
                            );
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{tdd.TaskName}] Error calling SnapshotState for CP {alignedBarrier.CheckpointId}: {ex.Message}");
                        // TODO: Fail the checkpoint for this task.
                    }
                }
                else
                {
                    Console.WriteLine($"[{tdd.TaskName}] Operator is NOT ICheckpointableOperator. Skipping state snapshot for CP {alignedBarrier.CheckpointId}.");
                }

                // Forward the barrier
                Console.WriteLine($"[{tdd.TaskName}] Forwarding aligned barrier {alignedBarrier.CheckpointId} to downstream operators.");
                foreach (INetworkedCollector collector in collectors) // 'collectors' is from ExecuteFromDescriptor's scope
                {
                    await collector.CollectObject(alignedBarrier, cancellationToken); // cancellationToken from ExecuteFromDescriptor
                }

                // After snapshotting and forwarding, the OperatorBarrierHandler will remove the CP state.
                // If snapshotting fails, the CP should be marked as failed for this task.
            };

            var barrierHandler = new OperatorBarrierHandler(
                tdd.JobVertexId,
                tdd.SubtaskIndex,
                expectedInputIds,
                onAlignedCallback);
            _operatorBarrierHandlers[$"{tdd.JobVertexId}_{tdd.SubtaskIndex}"] = barrierHandler;

            Console.WriteLine($"[{tdd.TaskName}] OPERATOR task started. Waiting for input via DataExchangeService. Barrier handler registered.");
            try { await Task.Delay(Timeout.Infinite, cancellationToken); } catch (OperationCanceledException) { Console.WriteLine($"[{tdd.TaskName}] Operator task canceled."); }
        }
        else if (isSink)
        {
             Console.WriteLine($"[{tdd.TaskName}] SINK task started. Waiting for input via DataExchangeService.");
             try { await Task.Delay(Timeout.Infinite, cancellationToken); } catch (OperationCanceledException) { Console.WriteLine($"[{tdd.TaskName}] Sink task canceled."); }
        }
        else {
             Console.WriteLine($"[{tdd.TaskName}] Component type not fully supported or no specific execution path.");
        }

        // Cleanup
        try
        {
            (operatorInstance as IOperatorLifecycle)?.Close();
            (sinkInstance as IOperatorLifecycle)?.Close();
            await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync())); // INetworkedCollector has CloseStreamAsync
        }
        finally
        {
             DataReceiverRegistry.UnregisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex);
             // Unregistration of source from ActiveTaskRegistry is handled by SourceTaskWrapper's ReportCompleted callback.
             _operatorBarrierHandlers.TryRemove($"{tdd.JobVertexId}_{tdd.SubtaskIndex}", out _);
             Console.WriteLine($"[{tdd.TaskName}] Execution finished and cleaned up.");
        }
    }

    private async Task RunSourceWithBarrierInjection<TOut>(
        ISourceFunction<TOut> sourceFunction,
        List<INetworkedCollector> collectors, // Changed from List<object>
        ChannelReader<BarrierInjectionRequest> barrierRequests,
        CancellationToken taskOverallCancellation,
        Action cancelSourceFunctionAction)
    {
        var sourceCts = CancellationTokenSource.CreateLinkedTokenSource(taskOverallCancellation);
        bool sourceRunCompleted = false;

        var sourceRunTask = Task.Run(() =>
        {
            try
            {
                var sourceContext = new SimpleSourceContext<TOut>(async record =>
                {
                    if (sourceCts.IsCancellationRequested) return;
                    foreach (INetworkedCollector collector in collectors)
                    {
                        // Assuming TOut is compatible with the 'object' record expected by CollectObject
                        // or that TOut is 'object' itself for sources.
                        await collector.CollectObject(record!, taskOverallCancellation);
                    }
                });
                sourceFunction.Run(sourceContext);
            }
            finally
            {
                sourceRunCompleted = true;
            }
        }, sourceCts.Token);

        try
        {
            while (!sourceRunCompleted && !taskOverallCancellation.IsCancellationRequested)
            {
                BarrierInjectionRequest? barrierRequest = null;
                try
                {
                    using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(taskOverallCancellation, timeoutCts.Token);
                    barrierRequest = await barrierRequests.ReadAsync(linkedCts.Token);
                }
                catch (OperationCanceledException) { /* Expected on timeout or task cancellation */ }
                catch (ChannelClosedException) { break; /* Channel closed, no more barriers */ }

                if (barrierRequest != null)
                {
                    Console.WriteLine($"TaskExecutor: Injecting barrier {barrierRequest.CheckpointId} for {sourceFunction.GetType().Name}");
                    var barrier = new FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier(
                        barrierRequest.CheckpointId,
                        barrierRequest.CheckpointTimestamp);

                    foreach (INetworkedCollector collector in collectors)
                    {
                        await collector.CollectObject(barrier, taskOverallCancellation);
                    }
                }

                if (sourceRunTask.IsCompleted || sourceRunTask.IsFaulted || sourceRunTask.IsCanceled)
                {
                    break;
                }
            }
        }
        finally
        {
            if (!sourceCts.IsCancellationRequested)
            {
                sourceCts.Cancel();
            }
            cancelSourceFunctionAction();
            await sourceRunTask;
            // SourceTaskWrapper.ReportCompleted() is handled by the unregister callback.
        }
    }
} // This brace closes TaskExecutor class

    // Example Map Operator for testing - define this in TaskManager project or make it discoverable
    public class SimpleStringToUpperMapOperator : IMapOperator<object, object>, IOperatorLifecycle, ICheckpointableOperator // Implement IOperatorLifecycle
    {
        private string _taskName = nameof(SimpleStringToUpperMapOperator);
        private IRuntimeContext? _context; // Store context if needed for state access later

        public void Open(IRuntimeContext context)
        {
            _taskName = context.TaskName;
            _context = context; // Store context
            Console.WriteLine($"[{_taskName}] SimpleStringToUpperMapOperator opened.");
        }
        public object Map(object record) => record.ToString()?.ToUpper() ?? "";
        public void Close() { Console.WriteLine($"[{_taskName}] SimpleStringToUpperMapOperator closed."); }

        // Implementation for ICheckpointableOperator
        public async Task<OperatorStateSnapshot> SnapshotState(long checkpointId, long checkpointTimestamp)
        {
            Console.WriteLine($"[{_taskName}] SnapshotState called for CP ID: {checkpointId}, Timestamp: {checkpointTimestamp}.");

            var snapshotStore = new FlinkDotNet.Storage.FileSystem.FileSystemSnapshotStore();
            var dummyStateData = System.Text.Encoding.UTF8.GetBytes($"State for operator {_taskName} (Instance: {_context?.TaskName}) at checkpoint {checkpointId} on {DateTime.UtcNow:o}");

            string currentTaskManagerId = FlinkDotNet.TaskManager.Program.TaskManagerId;
            string jobGraphJobId = _context!.JobName;
            string operatorInstanceId = _context!.TaskName;

            var snapshotHandleRecord = await snapshotStore.StoreSnapshot(
                jobGraphJobId,
                checkpointId,
                currentTaskManagerId,
                operatorInstanceId,
                dummyStateData);

            var snapshot = new OperatorStateSnapshot(snapshotHandleRecord.Value, dummyStateData.Length);
            snapshot.Metadata = new Dictionary<string, string>
            {
                { "OperatorType", _taskName }, // _taskName is operator type
                { "OperatorInstanceId", operatorInstanceId }, // operatorInstanceId is the unique instance name from context
                { "CheckpointTime", DateTime.UtcNow.ToString("o") },
                { "TaskManagerId", currentTaskManagerId }
            };
            Console.WriteLine($"[{_taskName}] Operator {operatorInstanceId} stored snapshot. Handle: {snapshot.StateHandle}, Size: {snapshot.StateSize}");
            return snapshot;
        }
    }
}
#nullable disable
