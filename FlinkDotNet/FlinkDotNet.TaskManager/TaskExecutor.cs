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
using FlinkDotNet.Proto.Internal;
using Grpc.Net.Client;
using Grpc.Core;
using Google.Protobuf;
using FlinkDotNet.TaskManager.Services;
using System.Collections.Generic;
using System.Diagnostics.Metrics; // Added for Metrics
using System.Globalization; // For CultureInfo


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
        // NetworkedCollector class definition from prompt
        public class NetworkedCollector<T>
        {
            private readonly string _sourceJobVertexId;
            private readonly int _sourceSubtaskIndex;
            private readonly OperatorOutput _outputInfo; // Changed from TaskDeploymentDescriptor.Types.OperatorOutput
            private readonly ITypeSerializer<T> _serializer;
            private readonly string _targetTmEndpoint;
            private DataExchangeService.DataExchangeServiceClient? _client;
            private AsyncClientStreamingCall<DataRecord, DataAck>? _streamCall;

            private const int MaxOutstandingSends = 100; // Or make it configurable
            private readonly SemaphoreSlim _sendPermits;

            public NetworkedCollector(
                string sourceJobVertexId, int sourceSubtaskIndex,
                OperatorOutput outputInfo,
                ITypeSerializer<T> serializer) // Removed targetTmEndpoint as it's in outputInfo
            {
                _sendPermits = new SemaphoreSlim(MaxOutstandingSends, MaxOutstandingSends); // Initialize semaphore
                _sourceJobVertexId = sourceJobVertexId;
                _sourceSubtaskIndex = sourceSubtaskIndex;
                _outputInfo = outputInfo;
                _serializer = serializer;
                _targetTmEndpoint = _outputInfo.TargetTaskEndpoint; // Get from outputInfo
                Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] NetworkedCollector created for target {_outputInfo.TargetVertexId} at {_targetTmEndpoint}");
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

                    if (record is string recordString && recordString.StartsWith("BARRIER_"))
                    {
                        // This is our PoC string barrier marker
                        Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] NetworkedCollector received string barrier marker: {recordString}");
                        try
                        {
                            string[] parts = recordString.Split('_');
                            long checkpointId = long.Parse(parts[1], CultureInfo.InvariantCulture);
                            long timestamp = long.Parse(parts[2], CultureInfo.InvariantCulture);
                            // bool isFinal = parts.Length > 3 && parts[3] == "FINAL"; // We can add options later

                            var barrierPayload = new CheckpointBarrier // FROM Proto namespace
                            {
                                CheckpointId = checkpointId,
                                CheckpointTimestamp = timestamp
                            };

                            dataRecordToSend = new DataRecord
                            {
                                TargetJobVertexId = _outputInfo.TargetVertexId,
                                TargetSubtaskIndex = _outputInfo.TargetSpecificSubtaskIndex,
                                IsCheckpointBarrier = true,
                                BarrierPayload = barrierPayload, // Assign to the oneof field
                                Payload = ByteString.Empty // Main payload is empty for barriers
                            };
                             Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] Converted string marker to Protobuf Barrier ID: {checkpointId}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[{_sourceJobVertexId}_{_sourceSubtaskIndex}] ERROR: Failed to parse string barrier marker '{recordString}': {ex.Message}. Sending as regular data.");
                            // Fallback: send as regular data if parsing fails, though this indicates an issue.
                            var payloadBytes = _serializer.Serialize(record);
                            dataRecordToSend = new DataRecord
                            {
                                TargetJobVertexId = _outputInfo.TargetVertexId,
                                TargetSubtaskIndex = _outputInfo.TargetSpecificSubtaskIndex,
                                IsCheckpointBarrier = false,
                                Payload = ByteString.CopyFrom(payloadBytes)
                            };
                        }
                    }
                    else
                    {
                        // Regular data record
                        var payloadBytes = _serializer.Serialize(record);
                        dataRecordToSend = new DataRecord
                        {
                            TargetJobVertexId = _outputInfo.TargetVertexId,
                            TargetSubtaskIndex = _outputInfo.TargetSpecificSubtaskIndex,
                            IsCheckpointBarrier = false,
                            Payload = ByteString.CopyFrom(payloadBytes)
                        };
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

        var collectors = new List<NetworkedCollector<object>>();

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
            if (outSerType != null) outputDataSerializer = Activator.CreateInstance(outSerType) as ITypeSerializer<object>;
            else { Console.WriteLine($"[{tdd.TaskName}] WARNING: Output serializer type '{tdd.OutputSerializerTypeName}' not found."); }

            if (outputDataSerializer == null) {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: Output serializer could not be instantiated for outputs.");
                 await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync())); // Close collectors if any were made
                 return;
            }

            foreach (var outputDesc in tdd.Outputs) // Use outputDesc from TDD
            {
                // targetTmEndpoint now comes from outputDesc.TargetTaskEndpoint
                Console.WriteLine($"[{tdd.TaskName}] Setting up NetworkedCollector for output to {outputDesc.TargetVertexId} at {outputDesc.TargetTaskEndpoint} (Subtask: {outputDesc.TargetSpecificSubtaskIndex})");
                collectors.Add(new NetworkedCollector<object>(
                    tdd.JobVertexId,
                    tdd.SubtaskIndex,
                    outputDesc,
                    outputDataSerializer
                ));
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

        // Register receiver for operators and sinks
        if ((isOperator || isSink) && inputDataSerializer != null)
        {
            ProcessRecordDelegate recordProcessor = async (targetJobVertexId, targetSubtaskIndex, payload) =>
            {
                if (targetJobVertexId != tdd.JobVertexId || targetSubtaskIndex != tdd.SubtaskIndex) return;
                try
                {
                    // METRICS_PLACEHOLDER:
                    // var currentTaskMetrics = TaskMetricsRegistry.Get(tdd.JobVertexId + "_" + tdd.SubtaskIndex);
                    // if (currentTaskMetrics != null) currentTaskMetrics.RecordsIn++;

                    var deserializedRecord = inputDataSerializer.Deserialize(payload);
                    if (isOperator && operatorInstance != null)
                    {
                        var result = operatorInstance.Map(deserializedRecord); // Assuming IMapOperator<object, object>
                        foreach (var collector in collectors) { await collector.Collect(result, cancellationToken); }
                    }
                    else if (isSink && sinkInstance != null)
                    {
                        sinkInstance.Invoke(deserializedRecord, new SimpleSinkContext());
                    }
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
            // The source needs to handle its generic type TOut and use the appropriate NetworkedCollector<TOut>.
            // This PoC uses NetworkedCollector<object> and ISourceFunction<object> or ISourceFunction<string> with special handling.
            try // Added try for source execution
            {
                if (sourceInstance is ISourceFunction<string> typedStringSourceNotNull && tdd.OutputTypeName == "System.String") // Renamed to avoid conflict
                {
                    var sCtxString = new SimpleSourceContext<string>(async record => {
                        foreach (var collector in collectors) { await collector.Collect(record, cancellationToken); }
                    });
                    await Task.Run(() => typedStringSourceNotNull.Run(sCtxString), cancellationToken);
                    typedStringSourceNotNull.Cancel();
                }
                else if (sourceInstance is ISourceFunction<object> objectSourceInstance) // Corrected variable name
                {
                    var sCtxObj = new SimpleSourceContext<object>(async record => {
                       foreach (var collector in collectors) { await collector.Collect(record, cancellationToken); }
                    });
                    await Task.Run(() => objectSourceInstance.Run(sCtxObj), cancellationToken);
                    objectSourceInstance.Cancel();
                }
                else
                {
                     Console.WriteLine($"[{tdd.TaskName}] Source instance type {sourceInstance?.GetType()} not directly runnable with current logic.");
                }
            } // Closed try
            catch (OperationCanceledException) { Console.WriteLine($"[{tdd.TaskName}] Source run canceled."); }
            catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] Error running source: {ex.Message} {ex.StackTrace}");}
        }
        else if (isOperator)
        {
             Console.WriteLine($"[{tdd.TaskName}] OPERATOR task started. Waiting for input via DataExchangeService.");
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
            await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync()));
        }
        finally
        {
             DataReceiverRegistry.UnregisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex);
             Console.WriteLine($"[{tdd.TaskName}] Execution finished and cleaned up.");
        }
    }
    } // This brace closes TaskExecutor class

    // Example Map Operator for testing - define this in TaskManager project or make it discoverable
    public class SimpleStringToUpperMapOperator : IMapOperator<object, object>, IOperatorLifecycle // Implement IOperatorLifecycle
    {
        private string _taskName = nameof(SimpleStringToUpperMapOperator);
        public void Open(IRuntimeContext context) { _taskName = context.TaskName; Console.WriteLine($"[{_taskName}] SimpleStringToUpperMapOperator opened."); }
        public object Map(object record) => record.ToString()?.ToUpper() ?? "";
        public void Close() { Console.WriteLine($"[{_taskName}] SimpleStringToUpperMapOperator closed."); }
    }
}
#nullable disable
