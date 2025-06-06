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
        Console.WriteLine($"[{tdd.TaskName}] Attempting to execute task from TDD. Head Operator: {tdd.FullyQualifiedOperatorName}");
        IRuntimeContext runtimeContext = new BasicRuntimeContext(
            jobName: tdd.JobGraphJobId,
            taskName: tdd.TaskName,
            numberOfParallelSubtasks: 0, // TODO: Get total parallelism for this vertex from TDD or other source
            indexOfThisSubtask: tdd.SubtaskIndex
        );

        var allOperatorInstances = new List<object>();
        var allCollectors = new List<object>(); // Will hold NetworkedCollector or placeholder for ChainedCollector

        // --- 1. Instantiate Head Operator ---
        object? headOperatorInstance = null;
        Type? headOperatorType = Type.GetType(tdd.FullyQualifiedOperatorName);

        if (headOperatorType == null)
        {
            Console.WriteLine($"[{tdd.TaskName}] ERROR: Head operator type '{tdd.FullyQualifiedOperatorName}' not found.");
            return; // Early exit
        }

        try
        {
            // Simplified instantiation for head operator. Configuration applied via properties or constructor.
            // This part needs to be robust. For now, assume default constructor or simple property-based config.
            // If operatorProperties are for head, they should be used here.
            // For PoC, we'll rely on specific type checks later like existing code.
            headOperatorInstance = Activator.CreateInstance(headOperatorType);
            if (headOperatorInstance == null) throw new InvalidOperationException("Activator.CreateInstance returned null");
            Console.WriteLine($"[{tdd.TaskName}] Head operator {tdd.FullyQualifiedOperatorName} instantiated.");
            allOperatorInstances.Add(headOperatorInstance);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to instantiate head operator {tdd.FullyQualifiedOperatorName}: {ex.Message}");
            return; // Early exit
        }

        // Determine head operator type (Source, Operator, Sink) for flow control
        // This is a simplified view for the head operator; chaining changes intermediate operators.
        bool isHeadSource = tdd.Inputs.Count == 0; // A source has no network inputs to the TDD itself
        // bool isHeadSink = tdd.Outputs.Count == 0 && tdd.ChainedOperatorInfo.Count == 0; // A sink has no network outputs from the TDD
        // bool isHeadOperator = !isHeadSource && !isHeadSink;

        // Original type determination for specific instance variables (can be refactored later)
        object? sourceInstance = null; // Will be headOperatorInstance if isHeadSource
        IMapOperator<object, object>? operatorInstance = null; // Will be headOperatorInstance if !isHeadSource and not a sink
        ISinkFunction<object>? sinkInstance = null; // Will be headOperatorInstance if isHeadSink (and not chained)

        // This logic is kept for now to align with existing instantiation details below,
        // but headOperatorInstance is the primary one for the chain.
        // Type? componentType = headOperatorType; // componentType is now headOperatorType. This line is confirmed as removed/unnecessary.

        // --- Instantiate Chained Operators ---
        if (tdd.ChainedOperatorInfo != null && tdd.ChainedOperatorInfo.Count > 0)
        {
            Console.WriteLine($"[{tdd.TaskName}] Instantiating {tdd.ChainedOperatorInfo.Count} chained operators.");
            foreach (var chainedOpProto in tdd.ChainedOperatorInfo)
            {
                Type? chainedOpType = Type.GetType(chainedOpProto.FullyQualifiedOperatorName);
                if (chainedOpType == null)
                {
                    Console.WriteLine($"[{tdd.TaskName}] ERROR: Chained operator type '{chainedOpProto.FullyQualifiedOperatorName}' not found.");
                    // Consider how to handle partial failure: cleanup already instantiated operators?
                    return; // Early exit
                }
                try
                {
                    // Assuming chainedOpProto.OperatorConfiguration are bytes of a JSON string for now
                    string configJson = System.Text.Encoding.UTF8.GetString(chainedOpProto.OperatorConfiguration.ToByteArray());
                    // TODO: Actual instantiation might need to pass this configJson to a constructor or property.
                    // For now, Activator.CreateInstance is used, assuming default constructor.
                    object chainedOpInstance = Activator.CreateInstance(chainedOpType);
                    if (chainedOpInstance == null) throw new InvalidOperationException("Activator.CreateInstance returned null for chained operator.");

                    // TODO: Apply configJson to the chainedOpInstance if necessary.
                    // This might involve checking for a specific interface/method or property.
                    // Example: if (chainedOpInstance is IConfigurableOperator configurable) { configurable.Configure(configJson); }

                    allOperatorInstances.Add(chainedOpInstance);
                    Console.WriteLine($"[{tdd.TaskName}] Chained operator {chainedOpProto.FullyQualifiedOperatorName} instantiated.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to instantiate chained operator {chainedOpProto.FullyQualifiedOperatorName}: {ex.Message}");
                    return; // Early exit
                }
            }
        }

        // --- 4. Lifecycle Management (Open) ---
        Console.WriteLine($"[{tdd.TaskName}] Opening {allOperatorInstances.Count} operator instance(s).");
        foreach (var opInstance in allOperatorInstances)
        {
            if (opInstance is IOperatorLifecycle lifecycle)
            {
                try
                {
                    lifecycle.Open(runtimeContext);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to open operator {opInstance.GetType().Name}: {ex.Message}");
                    // Consider cleanup of already opened operators if one fails to open.
                    return; // Early exit
                }
            }
        }

        // var collectors = new List<NetworkedCollector<object>>(); // Removed old 'collectors' list declaration

        // Original type determination variables like isSource, isOperator, isSink are kept for now
        // to minimize disruption to the existing serializer and component-specific instantiation logic below.
        // These will be refactored once the chain execution flow is fully in place.
        Type? componentType = headOperatorType; // Re-assign for compatibility with existing logic sections.
        bool isSource = tdd.Inputs.Count == 0 && tdd.Outputs.Count > 0; // Original logic
        bool isSink = tdd.Inputs.Count > 0 && tdd.Outputs.Count == 0;   // Original logic
        bool isOperator = tdd.Inputs.Count > 0 && tdd.Outputs.Count > 0; // Original logic

        // For Source/Operator: Setup output serializer and collectors
        // --- 5. Setup Collectors ---
        // Initialize allCollectors with placeholders (null for ChainedCollector)
        for (int i = 0; i < allOperatorInstances.Count; i++)
        {
            allCollectors.Add(null!); // Add null placeholder initially
        }

        ITypeSerializer<object>? outputDataSerializer = null;
        if (tdd.Outputs.Count > 0) // Only setup output serializer if there are network outputs for the whole task
        {
            if (string.IsNullOrEmpty(tdd.OutputSerializerTypeName) || string.IsNullOrEmpty(tdd.OutputTypeName))
            {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: Output type or serializer not defined for a task with outputs.");
                 return;
            }
            Type? outSerType = Type.GetType(tdd.OutputSerializerTypeName);
            if (outSerType != null) outputDataSerializer = Activator.CreateInstance(outSerType) as ITypeSerializer<object>;
            else { Console.WriteLine($"[{tdd.TaskName}] WARNING: Output serializer type '{tdd.OutputSerializerTypeName}' not found."); }

            if (outputDataSerializer == null) {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: Output serializer could not be instantiated for task outputs.");
                 return;
            }

            // Create CreditAwareTaskOutputs for the *last* operator in the chain if the task has outputs.
            if (allOperatorInstances.Count > 0)
            {
                var creditAwareOutputsForLastOp = new List<CreditAwareTaskOutput>();
                foreach (var outputDesc in tdd.Outputs)
                {
                    Console.WriteLine($"[{tdd.TaskName}] Setting up CreditAwareTaskOutput for task output to {outputDesc.TargetVertexId} at {outputDesc.TargetTaskEndpoint} (Subtask: {outputDesc.TargetSpecificSubtaskIndex})");
                    // Ensure outputDataSerializer is not null if tdd.Outputs.Count > 0 (already checked above)
                    var outputSender = new CreditAwareTaskOutput(
                        tdd.JobVertexId, // sourceJobVertexId for this output
                        tdd.SubtaskIndex, // sourceSubtaskIndex for this output
                        outputDesc,
                        outputDataSerializer!, // Pass the ITypeSerializer<object>, non-null asserted by check above
                        outputDesc.TargetTaskEndpoint
                    );
                    creditAwareOutputsForLastOp.Add(outputSender);
                }
                allCollectors[allOperatorInstances.Count - 1] = creditAwareOutputsForLastOp;
            }
        }
        // Now, populate them by iterating backwards.
        for (int i = allOperatorInstances.Count - 2; i >= 0; i--)
        {
            object currentOperator = allOperatorInstances[i];
            object nextOperator = allOperatorInstances[i+1];
            object? collectorForNextOperatorOutput = allCollectors[i+1];

            Type? outputTypeOfCurrentOperator = null;
            Type? inputTypeOfNextOperator = null;

            // Determine OutputTypeOfCurrentOperator
            if (i == 0) // Head operator
            {
                // If the head operator is part of a chain, its direct output type might differ from tdd.OutputTypeName (which is for the whole task)
                // We need the actual output type of the head operator instance.
                // For a source, it's tdd.OutputTypeName if not chained, or its specific output type if chained.
                // For an operator, it's its specific output type.
                // This requires knowing the head operator's actual output type.
                // Let's assume headOperatorInstance.GetType() and then find IMapOperator<TIn, TOut> or ISourceFunction<TOut>
                // For simplicity, if head is source, tdd.OutputTypeName is its initial output.
                // If head is operator, its output type is complex if not explicitly defined.
                // This part is tricky. Let's assume the TDD provides this.
                // The TDD.OutputTypeName is for the *last* operator in the *task*.
                // The TDD.ChainedOperatorInfo[0].InputTypeName is the input to the first *chained* op, so output of head.
                if (tdd.ChainedOperatorInfo.Count > 0 && i == 0) // Head operator followed by a chain
                {
                    // The output of the head operator must match the input of the first chained operator.
                     outputTypeOfCurrentOperator = Type.GetType(tdd.ChainedOperatorInfo[0].InputTypeName);
                     if (outputTypeOfCurrentOperator == null) {
                        Console.WriteLine($"[{tdd.TaskName}] ERROR: Cannot determine Output Type for Head Operator {currentOperator.GetType().Name} when it's chained. Expected input type for {tdd.ChainedOperatorInfo[0].FullyQualifiedOperatorName} was {tdd.ChainedOperatorInfo[0].InputTypeName}. Type resolution failed.");
                        return; // Critical error
                     }
                }
                else if (i == 0) // Head operator, NOT followed by a chain (single operator task)
                {
                    // Its output is the task's output
                    outputTypeOfCurrentOperator = Type.GetType(tdd.OutputTypeName);
                     if (outputTypeOfCurrentOperator == null && tdd.Outputs.Count > 0) { // Only error if it's supposed to have output
                        Console.WriteLine($"[{tdd.TaskName}] ERROR: Cannot determine Output Type for Head Operator {currentOperator.GetType().Name} (single operator). Task OutputTypeName: {tdd.OutputTypeName}. Type resolution failed.");
                        return; // Critical error
                     } else if (outputTypeOfCurrentOperator == null) { // e.g. a single sink operator
                        // This is fine, it means the operator doesn't produce output for a collector.
                        // ChainedCollector should not be created if outputTypeOfCurrentOperator is null.
                        Console.WriteLine($"[{tdd.TaskName}] Head operator {currentOperator.GetType().Name} has no discernible output type for a collector. Skipping ChainedCollector creation for it.");
                        continue;
                     }
                }
            }
            else // Current operator is a chained operator (not head)
            {
                outputTypeOfCurrentOperator = Type.GetType(tdd.ChainedOperatorInfo[i-1].OutputTypeName);
                 if (outputTypeOfCurrentOperator == null) {
                    Console.WriteLine($"[{tdd.TaskName}] ERROR: Cannot determine Output Type for Chained Operator {currentOperator.GetType().Name} from ChainedOperatorInfo (index {i-1}). OutputTypeName: {tdd.ChainedOperatorInfo[i-1].OutputTypeName}. Type resolution failed.");
                    return; // Critical error
                 }
            }

            // Determine InputTypeOfNextOperator
            // nextOperator is allOperatorInstances[i+1].
            // If i+1 is the first chained op, its info is in tdd.ChainedOperatorInfo[0].
            // Generally, for allOperatorInstances[i+1], its info is in tdd.ChainedOperatorInfo[ (i+1) - 1 = i ]
            inputTypeOfNextOperator = Type.GetType(tdd.ChainedOperatorInfo[i].InputTypeName);
            if (inputTypeOfNextOperator == null) {
                Console.WriteLine($"[{tdd.TaskName}] ERROR: Cannot determine Input Type for Next Operator {nextOperator.GetType().Name} from ChainedOperatorInfo (index {i}). InputTypeName: {tdd.ChainedOperatorInfo[i].InputTypeName}. Type resolution failed.");
                return; // Critical error
            }

            if (outputTypeOfCurrentOperator == null) // Should have been caught above if it was an error condition
            {
                 Console.WriteLine($"[{tdd.TaskName}] INFO: Output type of current operator {currentOperator.GetType().Name} is null, skipping ChainedCollector creation.");
                 allCollectors[i] = null!; // Ensure it's null if no collector
                 continue;
            }

            try
            {
                Type chainedCollectorGenericType = typeof(ChainedCollector<>);
                Type chainedCollectorTyped = chainedCollectorGenericType.MakeGenericType(outputTypeOfCurrentOperator);
                object chainedCollectorInstance = Activator.CreateInstance(chainedCollectorTyped, nextOperator, collectorForNextOperatorOutput, inputTypeOfNextOperator)!;
                allCollectors[i] = chainedCollectorInstance;
                Console.WriteLine($"[{tdd.TaskName}] Created ChainedCollector<{outputTypeOfCurrentOperator.Name}> for operator {currentOperator.GetType().Name} (index {i}). Next is {nextOperator.GetType().Name}. Next Op Input Type: {inputTypeOfNextOperator.Name}. Collector for next op output: {collectorForNextOperatorOutput?.GetType().Name ?? "null"}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to create ChainedCollector for output of {currentOperator.GetType().Name} (type {outputTypeOfCurrentOperator.Name}): {ex.Message} {ex.StackTrace}");
                return; // Critical error
            }
        }


        // For Operator/Sink: Setup input serializer and register receiver
        // This section is for the *head* operator of the chain if it takes network input.
        ITypeSerializer<object>? inputDataSerializer = null;
        if (isOperator || isSink) // Original condition, applies if head operator is not a source
        {
            if (string.IsNullOrEmpty(tdd.InputSerializerTypeName) || string.IsNullOrEmpty(tdd.InputTypeName))
            {
                Console.WriteLine($"[{tdd.TaskName}] ERROR: Input type or serializer not defined for a non-source task head.");
                // await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync()));
                return;
            }
            Type? inSerType = Type.GetType(tdd.InputSerializerTypeName);
            if (inSerType != null) inputDataSerializer = Activator.CreateInstance(inSerType) as ITypeSerializer<object>;
            else { Console.WriteLine($"[{tdd.TaskName}] WARNING: Input serializer type '{tdd.InputSerializerTypeName}' not found.");}

            if (inputDataSerializer == null) {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: Input serializer could not be instantiated for task input.");
                 // await Task.WhenAll(collectors.Select(c => c.CloseStreamAsync()));
                 return;
            }
        }

        // Instantiate the core component (Source, Operator, or Sink) - THIS PART NEEDS REWORK for headOperatorInstance
        // The headOperatorInstance is already in allOperatorInstances[0].
        // The specific type checks (FileSourceFunction, SimpleStringToUpperMapOperator, etc.)
        // are for applying specific configurations or constructors which might be complex with Activator.CreateInstance alone.
        // This logic will need to be adapted to configure allOperatorInstances[0] if special logic is needed beyond Activator.CreateInstance.

        // Example of adapting the old logic for headOperatorInstance (allOperatorInstances[0])
        if (isSource)
        {
            sourceInstance = allOperatorInstances[0]; // Assign from the list
            // Specific configuration for FileSourceFunction example
            if (headOperatorInstance is ISourceFunction<string> && // Check actual type
                tdd.FullyQualifiedOperatorName.StartsWith("FlinkDotNet.Connectors.Sources.File.FileSourceFunction`1") &&
                tdd.OutputTypeName == "System.String")
            {
                // This is tricky because FileSourceFunction takes filePath and serializer in constructor.
                // Activator.CreateInstance earlier used default constructor.
                // This requires either re-instantiation or property-based configuration.
                // For now, this highlights a gap if complex constructors are needed for head operator.
                // Let's assume properties can be set or a re-instantiation happens if specific constructor needed.
                // The current code instantiated with default constructor. If that's not enough, it needs adjustment.
                Console.WriteLine($"[{tdd.TaskName}] Head operator is a Source. Specific constructor logic for FileSource might need review if default constructor is not used/sufficient.");
                 var stringSerializerForSource = Activator.CreateInstance(Type.GetType(tdd.OutputSerializerTypeName!)!) as ITypeSerializer<string>;
                 if (stringSerializerForSource != null && operatorProperties.TryGetValue("filePath", out var path)) {
                    // Re-instantiate IF NEEDED (this is a simplified example of handling specific constructor)
                    // For a real system, use a factory or DI that handles parameters.
                    // allOperatorInstances[0] = Activator.CreateInstance(componentType.MakeGenericType(typeof(string)), path, stringSerializerForSource);
                    // sourceInstance = allOperatorInstances[0];
                    Console.WriteLine($"[{tdd.TaskName}] FileSourceFunction specific setup would go here. Current head instance: {sourceInstance?.GetType().Name}");
                 } else {
                      Console.WriteLine($"[{tdd.TaskName}] Could not get stringSerializerForSource or filePath for FileSourceFunction.");
                 }
            }
        }
        else if (isOperator) // Head is an operator
        {
            operatorInstance = allOperatorInstances[0] as IMapOperator<object, object>; // Assign from list
            // Lifecycle Open already called for all instances, including this one.
        }
        else if (isSink) // Head is a sink
        {
            sinkInstance = allOperatorInstances[0] as ISinkFunction<object>; // Assign from list
            // Lifecycle Open already called for all instances.
        }

        // Check if head operator was successfully cast to its role variable (sourceInstance, operatorInstance, sinkInstance)
        // This is more of a check on the logic assigning these variables than on instantiation itself, which was checked earlier.
        if ((isSource && sourceInstance == null && allOperatorInstances.Count > 0 && allOperatorInstances[0] is ISourceFunction<object>) || // If it's a generic source
            (isOperator && operatorInstance == null && allOperatorInstances.Count > 0) ||
            (isSink && sinkInstance == null && allOperatorInstances.Count > 0 ))
        {
             // If headOperatorInstance was not null but the role-specific variable is, it means the type check/cast failed.
             // However, allOperatorInstances[0] holds the successfully instantiated head operator.
             // The specific variables sourceInstance, operatorInstance, sinkInstance are becoming less relevant for the chain's head.
             Console.WriteLine($"[{tdd.TaskName}] Successfully instantiated head operator: {allOperatorInstances[0].GetType().FullName}. Role-specific variable might be null if type is generic/unexpected by old logic.");
        } else if (allOperatorInstances.Count == 0) {
             Console.WriteLine($"[{tdd.TaskName}] ERROR: Head operator failed to be part of allOperatorInstances list after instantiation attempts.");
             return;
        }


        Console.WriteLine($"[{tdd.TaskName}] All operators ({allOperatorInstances.Count}) instantiated and opened successfully.");

        // Register receiver for operators and sinks - This applies if the HEAD of the chain is not a source.
        if (!isHeadSource && inputDataSerializer != null) // Modified condition to use isHeadSource
        {
            ProcessRecordDelegate recordProcessor = async (targetJobVertexId, targetSubtaskIndex, payload) =>
            {
                if (targetJobVertexId != tdd.JobVertexId || targetSubtaskIndex != tdd.SubtaskIndex) return;
                try
                {
                    var deserializedRecord = inputDataSerializer.Deserialize(payload);

                    if (allCollectors.Count > 0 && allCollectors[0] is ICollector<object> headCollector)
                    {
                        // Assuming InputTypeName of TDD matches the TIn of the headCollector.
                        // This cast to ICollector<object> is a simplification.
                        // Ideally, TIn of headCollector matches typeof(deserializedRecord).
                        headCollector.Collect(deserializedRecord);
                    }
                    else if (allCollectors.Count == 0 && allOperatorInstances.Count > 0) // Single operator, possibly a sink with no collector.
                    {
                         // This case implies the head operator is a sink and does not propagate data further via collectors.
                         // Its processing (if any beyond Invoke) would have been handled by the loop if it existed.
                         // Or, if it's a simple IMapOperator that's the only one and has no output, its result is dropped.
                         // For a sink, direct invocation might be needed if it's not handled by a collector path.
                        var headOp = allOperatorInstances[0];
                        if (headOp is ISinkFunction<object> sinkFn)
                        {
                            sinkFn.Invoke(deserializedRecord, new SimpleSinkContext());
                        }
                        else
                        {
                             Console.WriteLine($"[{tdd.TaskName}] Received input for a task head operator ({headOp.GetType().Name}) that has no configured collector and is not a direct sink. Input dropped.");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[{tdd.TaskName}] ERROR: Head collector not found or not of expected type ICollector<object> for network input.");
                    }
                }
                catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] Error processing received record via head collector: {ex.Message} {ex.StackTrace}"); }
            };
            DataReceiverRegistry.RegisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex, recordProcessor);
            cancellationToken.Register(() => DataReceiverRegistry.UnregisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex));
        }


        // --- Execution Logic ---
        // If head is ISourceFunction
        if (isHeadSource && allOperatorInstances.Count > 0 && allOperatorInstances[0] is ISourceFunction<object> headSourceInstance)
        {
            Console.WriteLine($"[{tdd.TaskName}] Running as a SOURCE task with head: {headSourceInstance.GetType().Name}.");
            try
            {
                // The collector for the source (allOperatorInstances[0]) is allCollectors[0].
                // If chain length > 1, allCollectors[0] is null (for ChainedCollector).
                // If chain length == 1 and has output, allCollectors[0] is List<NetworkedCollector>.
                if (allOperatorInstances.Count > 1) {
                     Console.WriteLine($"[{tdd.TaskName}] Source output will effectively go to the next operator in chain.");
                } else if (allCollectors.Count > 0 && allCollectors[0] != null) { // Single operator source, with network output
                     Console.WriteLine($"[{tdd.TaskName}] Source output will go to NetworkedCollector(s).");
                }

                var sCtxObj = new SimpleSourceContext<object>(recordFromSource => { // Assuming source output is object for simplicity with ICollector<object>
                    if (allCollectors.Count > 0 && allCollectors[0] is ICollector<object> sourceOutputCollector)
                    {
                        sourceOutputCollector.Collect(recordFromSource);
                    }
                    else if (allCollectors.Count == 0 && allOperatorInstances.Count == 1 && allOperatorInstances[0] is ISinkFunction<object> sinkSource)
                    {
                        Console.WriteLine($"[{tdd.TaskName}] Source is a direct sink. Invoking directly. Record: {recordFromSource}");
                        sinkSource.Invoke(recordFromSource, new SimpleSinkContext());
                    }
                    else
                    {
                        string collectorTypeInfo = (allCollectors.Count > 0 && allCollectors[0] != null) ? allCollectors[0]!.GetType().Name : "null";
                        Console.WriteLine($"[{tdd.TaskName}] Source emitted record, but collector at allCollectors[0] is '{collectorTypeInfo}' or misconfigured. Operators: {allOperatorInstances.Count}. Record dropped: {recordFromSource}");
                    }
                });

                // Before running the source or registering receiver, connect all CreditAwareTaskOutputs
                List<Task> connectTasks = new List<Task>();
                if (allCollectors.Count > 0 && allCollectors.LastOrDefault() is List<CreditAwareTaskOutput> outputSenders)
                {
                    Console.WriteLine($"[{tdd.TaskName}] Attempting to connect {outputSenders.Count} CreditAwareTaskOutput(s).");
                    foreach (var sender in outputSenders)
                    {
                        connectTasks.Add(sender.ConnectAsync(cancellationToken));
                    }
                }

                // Also connect any CreditAwareTaskOutput that might be used by intermediate ChainedCollectors
                // (though less common for an intermediate collector to be CreditAwareTaskOutput directly,
                // it's more likely the ChainedCollector's *own* downstream collector is a list of CreditAwareTaskOutput).
                // For now, this focuses on the final output collectors.

                try
                {
                    if (connectTasks.Count > 0) await Task.WhenAll(connectTasks);
                    Console.WriteLine($"[{tdd.TaskName}] All CreditAwareTaskOutputs connected (if any).");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to connect one or more CreditAwareTaskOutputs: {ex.Message}. Task will not run.");
                    if (allCollectors.Count > 0 && allCollectors.LastOrDefault() is List<CreditAwareTaskOutput> createdSenders)
                    {
                        foreach (var sender in createdSenders) await sender.CloseAsync(); // Attempt to clean up
                    }
                    return;
                }

                await Task.Run(() => headSourceInstance.Run(sCtxObj), cancellationToken);
                headSourceInstance.Cancel(); // Assuming ISourceFunction has Cancel
            }
            catch (OperationCanceledException) { Console.WriteLine($"[{tdd.TaskName}] Source run canceled."); }
            catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] Error running source chain: {ex.Message} {ex.StackTrace}");}
        }
        else if (!isHeadSource) // Head is an operator or sink, waiting for network input
        {
             Console.WriteLine($"[{tdd.TaskName}] Task (head: {allOperatorInstances[0].GetType().Name}) waiting for input via DataExchangeService.");
             try { await Task.Delay(Timeout.Infinite, cancellationToken); } catch (OperationCanceledException) { Console.WriteLine($"[{tdd.TaskName}] Task canceled."); }
        }
        else // Should not happen if logic is correct
        {
             Console.WriteLine($"[{tdd.TaskName}] Component type not fully supported or no specific execution path for head: {allOperatorInstances[0].GetType().Name}.");
        }

        // Cleanup
        try
        {
            // Close operators in reverse order
            for (int i = allOperatorInstances.Count - 1; i >= 0; i--)
            {
                if (allOperatorInstances[i] is IOperatorLifecycle lifecycle)
                {
                    lifecycle.Close();
                }
            }
            // Close CreditAwareTaskOutputs
            List<Task> closeTasks = new List<Task>();
            if (allCollectors.Count > 0 && allCollectors.LastOrDefault() is List<CreditAwareTaskOutput> finalOutputSenders)
            {
                Console.WriteLine($"[{tdd.TaskName}] Closing {finalOutputSenders.Count} CreditAwareTaskOutputs.");
                foreach (var sender in finalOutputSenders)
                {
                    closeTasks.Add(sender.CloseAsync());
                }
            }
            // Add other collector types that need async cleanup to closeTasks if necessary
            if(closeTasks.Count > 0) await Task.WhenAll(closeTasks);
        }
        finally
        {
             if (!isHeadSource) DataReceiverRegistry.UnregisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex);
             Console.WriteLine($"[{tdd.TaskName}] Execution finished and cleaned up for {allOperatorInstances.Count} operators.");
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
