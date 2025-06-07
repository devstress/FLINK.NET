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

using FlinkDotNet.Core.Abstractions.Execution; // For SerializerRegistry
using FlinkDotNet.Core.Abstractions.Models.Checkpointing; // For CheckpointBarrier (from Abstractions)
using System.Collections.Concurrent; // For ConcurrentDictionary
using FlinkDotNet.Core.Abstractions.Storage; // For IStateSnapshotStore (conceptual)

// LOGGING_PLACEHOLDER:
    // private readonly Microsoft.Extensions.Logging.ILogger<TaskExecutor> _logger; // Inject via constructor, ensure using Microsoft.Extensions.Logging;

    // Conceptual interface for barrier handling, to be defined properly if not already.
    // This would typically reside in a Core.Abstractions or similar project.
    public interface IOperatorBarrierHandler : IDisposable
    {
        Task HandleIncomingBarrier(long checkpointId, long checkpointTimestamp, string sourceInputId);
        // Task AllBarriersReceived(long checkpointId); // Example, actual signature might vary
        // Consider methods for timeout, abort notifications from JM etc.
        void RegisterInputs(List<string> expectedInputIds); // Added for setup
        void TriggerCheckpoint(long checkpointId, long timestamp); // Added for triggering by JM
    }

    // Placeholder for OperatorBarrierHandler from 'main' branch
    public class OperatorBarrierHandler : IOperatorBarrierHandler
    {
        private readonly string _jobVertexId;
        private readonly int _subtaskIndex;
        private List<string> _expectedInputIds;
        private readonly Func<FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier, Task> _onAlignedCallback;
        private readonly ConcurrentDictionary<long, ConcurrentDictionary<string, bool>> _receivedBarriers =
            new ConcurrentDictionary<long, ConcurrentDictionary<string, bool>>();

        public OperatorBarrierHandler(
            string jobVertexId,
            int subtaskIndex,
            List<string> expectedInputIds,
            Func<FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier, Task> onAlignedCallback)
        {
            _jobVertexId = jobVertexId;
            _subtaskIndex = subtaskIndex;
            _expectedInputIds = expectedInputIds ?? new List<string>();
            _onAlignedCallback = onAlignedCallback;
            Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] OperatorBarrierHandler created. Expecting inputs: {string.Join(", ", _expectedInputIds)}");
        }

        public void RegisterInputs(List<string> expectedInputIds)
        {
            _expectedInputIds = expectedInputIds ?? new List<string>();
            Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] OperatorBarrierHandler inputs registered: {string.Join(", ", _expectedInputIds)}");
        }

        public void TriggerCheckpoint(long checkpointId, long timestamp)
        {
            // This might be called by TM if this operator is a source for barriers in a different context
            // or if JobManager directly instructs this handler. For now, focused on HandleIncomingBarrier.
             Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] OperatorBarrierHandler TriggerCheckpoint called for CP ID: {checkpointId}. This is unusual for non-source OBH.");
        }

        public async Task HandleIncomingBarrier(long checkpointId, long checkpointTimestamp, string sourceInputId)
        {
            Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] OperatorBarrierHandler received barrier for CP ID: {checkpointId} from {sourceInputId}");
            var checkpointBarriers = _receivedBarriers.GetOrAdd(checkpointId, (_) => new ConcurrentDictionary<string, bool>());

            if (_expectedInputIds.Contains(sourceInputId))
            {
                checkpointBarriers[sourceInputId] = true;
            }
            else
            {
                Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] Warning: Received barrier from unexpected source {sourceInputId} for CP {checkpointId}.");
                // Potentially ignore or handle as error based on strictness.
            }

            // Check for alignment
            bool allAligned = _expectedInputIds.Count > 0 && _expectedInputIds.All(id => checkpointBarriers.ContainsKey(id) && checkpointBarriers[id]);

            if (allAligned)
            {
                Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] All barriers aligned for CP ID: {checkpointId}. Triggering onAlignedCallback.");
                var barrierToCallback = new FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier
                {
                    CheckpointId = checkpointId,
                    Timestamp = checkpointTimestamp,
                    // Options can be added if needed
                };
                await _onAlignedCallback(barrierToCallback);
                _receivedBarriers.TryRemove(checkpointId, out _); // Clean up for this checkpoint
            }
            else
            {
                 var receivedCount = checkpointBarriers.Count(kv => _expectedInputIds.Contains(kv.Key));
                 Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] CP ID: {checkpointId} not yet aligned. Received {receivedCount}/{_expectedInputIds.Count}. Waiting for others.");
                 // TODO: Implement alignment timeout logic here.
            }
        }

        public void Dispose()
        {
            Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] OperatorBarrierHandler disposed.");
            _receivedBarriers.Clear();
        }
    }

    // Placeholder for ActiveTaskRegistry if not defined elsewhere for this context
    public class ActiveTaskRegistry {
        public void RegisterTask(TaskExecutor executor, string jobId, string vertexId, int subtaskIndex, string taskName) {
            Console.WriteLine($"[ActiveTaskRegistry] Task {taskName} ({jobId}/{vertexId}_{subtaskIndex}) registered with executor {executor.GetHashCode()}.");
        }
        public void UnregisterTask(string jobId, string vertexId, int subtaskIndex, string taskName) {
            Console.WriteLine($"[ActiveTaskRegistry] Task {taskName} ({jobId}/{vertexId}_{subtaskIndex}) unregistered.");
        }
    }


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

    // Placeholder for SourceTaskWrapper from 'main' branch
    internal class SourceTaskWrapper<TOut>
    {
        public ISourceFunction<TOut> SourceFunction { get; }
        public string JobId { get; }
        public string VertexId { get; }
        public int SubtaskIndex { get; }
        public string TaskName { get; }
        // Potentially other fields like checkpointing service, barrier emission logic etc.

        public SourceTaskWrapper(ISourceFunction<TOut> sourceFunction, string jobId, string vertexId, int subtaskIndex, string taskName)
        {
            SourceFunction = sourceFunction;
            JobId = jobId;
            VertexId = vertexId;
            SubtaskIndex = subtaskIndex;
            TaskName = taskName;
        }

        public void Cancel()
        {
            SourceFunction.Cancel();
        }
    }

    public class TaskExecutor
    {
        private readonly ActiveTaskRegistry _activeTaskRegistry;
        private readonly TaskManagerCheckpointingServiceImpl _checkpointingService;
        private readonly SerializerRegistry _serializerRegistry;
        private readonly string _taskManagerId;
        private readonly IStateSnapshotStore _stateStore; // For state restoration

        private readonly ConcurrentDictionary<string, IOperatorBarrierHandler> _operatorBarrierHandlers =
            new ConcurrentDictionary<string, IOperatorBarrierHandler>();

        public TaskExecutor(
            ActiveTaskRegistry activeTaskRegistry,
            TaskManagerCheckpointingServiceImpl checkpointingService,
            SerializerRegistry serializerRegistry,
            string taskManagerId,
            IStateSnapshotStore stateStore) // Added stateStore
        {
            _activeTaskRegistry = activeTaskRegistry ?? throw new ArgumentNullException(nameof(activeTaskRegistry));
            _checkpointingService = checkpointingService ?? throw new ArgumentNullException(nameof(checkpointingService));
            _serializerRegistry = serializerRegistry ?? throw new ArgumentNullException(nameof(serializerRegistry));
            _taskManagerId = taskManagerId ?? throw new ArgumentNullException(nameof(taskManagerId));
            _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore)); // Store stateStore
        }

        public IOperatorBarrierHandler? GetOperatorBarrierHandler(string jobVertexId, int subtaskIndex)
        {
            // Ensure a consistent key format if JobGraphJobId is part of it in other places (e.g. OBH creation)
            _operatorBarrierHandlers.TryGetValue($"{jobVertexId}_{subtaskIndex}", out var handler);
            return handler;
        }

        // Placeholder for RunSourceWithBarrierInjection from 'main' branch
        private async Task RunSourceWithBarrierInjection<TOut>(
            ISourceFunction<TOut> sourceFunction,
            object outputCollectorOrSenders, // This is allCollectors[0]. Can be ChainedCollector or List<CreditAwareTaskOutput>
            IRuntimeContext runtimeContext,
            CancellationToken cancellationToken)
        {
            Console.WriteLine($"[{runtimeContext.TaskName}] Entered RunSourceWithBarrierInjection<{typeof(TOut).Name}>. Output target: {outputCollectorOrSenders?.GetType().Name ?? "null"}");

            // Conceptual Logic:
            // 1. Create SourceTaskWrapper (already done before calling this via reflection).
            // 2. Register with ActiveTaskRegistry (already done before calling this).
            // 3. Setup barrier injection:
            //    - The source itself doesn't have an OperatorBarrierHandler if it has no inputs.
            //    - It needs to listen for checkpoint triggers from TaskManagerCheckpointingServiceImpl.
            //    - When a checkpoint is triggered for this source task:
            //        a. It should emit a CheckpointBarrier object into its output stream(s).
            //        b. Then, it should trigger its own state snapshot if it's ICheckpointableOperator.
            //        c. Report snapshot completion to TaskManagerCheckpointingServiceImpl.

            // This simplified version just runs the source and uses SimpleSourceContext.
            // Barrier injection into the output collectors needs to be handled here.

            Action<TOut> actualCollectAction;
            if (outputCollectorOrSenders is ICollector<TOut> typedCollector)
            {
                actualCollectAction = typedCollector.Collect;
            }
            else if (outputCollectorOrSenders is Core.Abstractions.Collectors.ICollector<object> objCollector && typeof(TOut) == typeof(object))
            {
                 // This case handles if sourceOutputType was object and allCollectors[0] is ICollector<object>
                 actualCollectAction = (record) => objCollector.Collect(record!);
            }
            else if (outputCollectorOrSenders is List<NetworkedCollector<TOut>> networkSenders)
            {
                actualCollectAction = (record) => {
                    // This is for data records. Barriers are handled differently by RunSourceWithBarrierInjection.
                    foreach (var sender in networkSenders) {
                        // Assuming TOut can be passed to NetworkedCollector<TOut>.Collect directly
                        sender.Collect(record, isBarrier: false, 0, 0);
                    }
                };
            }
            else
            {
                Console.WriteLine($"[{runtimeContext.TaskName}] RunSourceWithBarrierInjection: Output collector type {outputCollectorOrSenders?.GetType().Name} not directly supported for TOut {typeof(TOut).Name} for data. Data will be dropped.");
                actualCollectAction = (record) => { /*noop*/ };
            }

            var sCtx = new SimpleSourceContext<TOut>(actualCollectAction);

            // TODO: Hook into _checkpointingService to listen for checkpoint triggers.
            // Conceptual: CheckpointTriggerListener triggerListener = (checkpointId, timestamp) => {
            //    Console.WriteLine($"[{runtimeContext.TaskName}] Source received trigger for CP {checkpointId}");
            //
            //    // 1. Emit Barrier to all outputs
            //    if (outputCollectorOrSenders is List<NetworkedCollector<TOut>> ns) {
            //        foreach (var sender in ns) {
            //            // Create a dummy TOut for barrier or use a specific barrier method if available
            //            sender.Collect(default(TOut)!, isBarrier: true, checkpointId: checkpointId, checkpointTimestamp: timestamp);
            //        }
            //    } else if (outputCollectorOrSenders is ICollector<TOut> coll) {
            //        // Need a way to send barrier marker through ICollector<TOut>
            //        // This might involve TOut being object or a special wrapper type.
            //        // For now, this highlights a gap for chained sources.
            //        Console.WriteLine($"[{runtimeContext.TaskName}] Barrier forwarding for chained source (ICollector<{typeof(TOut).Name}>) not fully implemented in placeholder.");
            //        if (coll is Core.Abstractions.Collectors.ICollector<object> objColl && typeof(TOut)==typeof(object)) {
            //             var barrierObj = new Core.Abstractions.Models.Checkpointing.CheckpointBarrier { CheckpointId = checkpointId, Timestamp = timestamp};
            //             objColl.Collect(barrierObj); // Send barrier as a special object
            //        }
            //    }
            //
            //    // 2. Snapshot State
            //    if (sourceFunction is ICheckpointableOperator checkpointable) {
            //        // await checkpointable.SnapshotState(snapshotContext);
            //        Console.WriteLine($"[{runtimeContext.TaskName}] PLACEHOLDER: Source operator {sourceFunction.GetType().Name} would snapshot state for CP {checkpointId}.");
            //    }
            //
            //    // 3. Notify Checkpointing Service
            //    // await _checkpointingService.AcknowledgeCheckpointAsync(...);
            //    Console.WriteLine($"[{runtimeContext.TaskName}] PLACEHOLDER: Source operator would acknowledge CP {checkpointId} to TM Checkpointing Service.");
            //};
            // _checkpointingService.RegisterCheckpointListener(tdd.JobVertexId, tdd.SubtaskIndex, triggerListener); // Conceptual

            try
            {
                await Task.Run(() => sourceFunction.Run(sCtx), cancellationToken);
                sourceFunction.Cancel(); // Ensure cancellation is called
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"[{runtimeContext.TaskName}] Source run cancelled within RunSourceWithBarrierInjection.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{runtimeContext.TaskName}] Error running source in RunSourceWithBarrierInjection: {ex.Message} {ex.StackTrace}");
            }
            finally
            {
                Console.WriteLine($"[{runtimeContext.TaskName}] RunSourceWithBarrierInjection for {sourceFunction.GetType().Name} finished.");
                // Unregister source task? Or done in ExecuteFromDescriptor's finally?
                // _activeTaskRegistry.UnregisterSourceTask(JobId, VertexId, SubtaskIndex); // Conceptual
            }
        }

        // NetworkedCollector class, adapted to be CreditAwareTaskOutput
        // For simplicity, TKey is removed from class signature but keying logic can be inside Collect if needed.
        public class NetworkedCollector<T> // This was CreditAwareTaskOutput, renamed to NetworkedCollector to fit existing TaskExecutor structure
        {
            private readonly string _sourceJobVertexId;
            private readonly int _sourceSubtaskIndex;
            private readonly OperatorOutput _outputInfo;
            private readonly ITypeSerializer<T> _serializer; // Changed from ITypeSerializer<object> to ITypeSerializer<T>
            private readonly string _targetTmEndpoint;

            private GrpcChannel? _channel;
            private DataExchangeService.DataExchangeServiceClient? _client; // Kept
            private AsyncDuplexStreamingCall<UpstreamPayload, DownstreamPayload>? _exchangeCall; // New

            private int _availableCredits = 0;
            private readonly ConcurrentQueue<DataRecord> _sendQueue = new ConcurrentQueue<DataRecord>();
            private readonly ManualResetEventSlim _sendSignal = new ManualResetEventSlim(false);

            private Task? _processSendQueueTask;
            private Task? _processDownstreamMessagesTask;
            private readonly CancellationTokenSource _ctsInternal = new CancellationTokenSource();
            private CancellationToken _linkedCt = CancellationToken.None;
            private readonly string _identifier;
            // private readonly SemaphoreSlim _sendPermits; // Optional: Kept for local concurrency limiting if desired

            public NetworkedCollector(
                string sourceJobVertexId, int sourceSubtaskIndex,
                OperatorOutput outputInfo,
                ITypeSerializer<T> serializer) // Changed to ITypeSerializer<T>
            {
                // _sendPermits = new SemaphoreSlim(100, 100); // Keep if local send concurrency limiting is useful
                _sourceJobVertexId = sourceJobVertexId;
                _sourceSubtaskIndex = sourceSubtaskIndex;
                _outputInfo = outputInfo;
                _serializer = serializer;
                _targetTmEndpoint = _outputInfo.TargetTaskEndpoint;
                _identifier = $"NetworkCollector-{_sourceJobVertexId}_{_sourceSubtaskIndex}->{_outputInfo.TargetVertexId}_{_outputInfo.TargetSpecificSubtaskIndex}";
                Console.WriteLine($"[{_identifier}] Created for target {_targetTmEndpoint}");
            }

            public async Task ConnectAsync(CancellationToken externalCt)
            {
                _linkedCt = CancellationTokenSource.CreateLinkedTokenSource(externalCt, _ctsInternal.Token).Token;

                if (_exchangeCall != null) {
                    Console.WriteLine($"[{_identifier}] ConnectAsync called but already connected or connecting.");
                    return;
                }

                Console.WriteLine($"[{_identifier}] Connecting to {_targetTmEndpoint}...");
                try
                {
                    _channel = GrpcChannel.ForAddress(_targetTmEndpoint);
                    _client = new DataExchangeService.DataExchangeServiceClient(_channel);
                    _exchangeCall = _client.ExchangeData(cancellationToken: _linkedCt);

                    _processDownstreamMessagesTask = Task.Run(() => ProcessDownstreamMessagesAsync(_linkedCt));
                    _processSendQueueTask = Task.Run(() => ProcessSendQueueAsync(_linkedCt));

                    Console.WriteLine($"[{_identifier}] Successfully connected. Background tasks started.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{_identifier}] Connection to {_targetTmEndpoint} failed: {ex.Message}");
                    _exchangeCall = null;
                    _channel = null;
                    if (!_ctsInternal.IsCancellationRequested) _ctsInternal.Cancel();
                    throw;
                }
            }

            // This method replaces the old Collect and acts like SendRecord from CreditAwareTaskOutput
            public void Collect(T record, bool isBarrier = false, long checkpointId = 0, long checkpointTimestamp = 0)
            {
                if (_ctsInternal.IsCancellationRequested) return;

                byte[] payloadBytes = _serializer.Serialize(record);
                var dataRecord = new DataRecord
                {
                    TargetJobVertexId = _outputInfo.TargetVertexId,
                    TargetSubtaskIndex = _outputInfo.TargetSpecificSubtaskIndex, // Keyed routing would modify this
                    SourceJobVertexId = _sourceJobVertexId,
                    SourceSubtaskIndex = _sourceSubtaskIndex,
                    IsCheckpointBarrier = isBarrier,
                    Payload = ByteString.CopyFrom(payloadBytes)
                };

                if (isBarrier)
                {
                    dataRecord.BarrierPayload = new CheckpointBarrier { CheckpointId = checkpointId, CheckpointTimestamp = checkpointTimestamp };
                    if (record is string s && s.StartsWith("BARRIER_")) // Compatibility with old string barrier format if Collect is called directly
                    {
                         try {
                            string[] parts = s.Split('_');
                            dataRecord.BarrierPayload.CheckpointId = long.Parse(parts[1], CultureInfo.InvariantCulture);
                            dataRecord.BarrierPayload.CheckpointTimestamp = long.Parse(parts[2], CultureInfo.InvariantCulture);
                            dataRecord.Payload = ByteString.Empty; // Barriers typically have no data payload
                         } catch (Exception ex) {
                             Console.WriteLine($"[{_identifier}] Error parsing string barrier '{s}': {ex.Message}. Sending as data.");
                             dataRecord.IsCheckpointBarrier = false; // Send as data if parse fails
                             dataRecord.Payload = ByteString.CopyFrom(_serializer.Serialize(record)); // Re-serialize original record
                         }
                    } else {
                         dataRecord.Payload = ByteString.Empty; // Ensure payload is empty for non-string barriers too
                    }
                }

                // TODO: Keyed routing logic if applicable, to set TargetSubtaskIndex
                // if (_keySelectorFunc != null && _keyingInfo != null) { ... dataRecord.TargetSubtaskIndex = ... }


                _sendQueue.Enqueue(dataRecord);
                _sendSignal.Set();
            }


            private async Task ProcessSendQueueAsync(CancellationToken ct)
            {
                Console.WriteLine($"[{_identifier}] Send queue processing task started.");
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        try { _sendSignal.Wait(ct); } catch (OperationCanceledException) { break; }
                        if (ct.IsCancellationRequested) break;

                        bool processedItemInLock = false;
                        lock (_sendSignal) // Lock to make credit check + dequeue atomic with signal reset
                        {
                            if (ct.IsCancellationRequested) break;
                            if (_availableCredits > 0 && _sendQueue.TryDequeue(out DataRecord? dataRecord))
                            {
                                Interlocked.Decrement(ref _availableCredits);
                                processedItemInLock = true;

                                _ = Task.Run(async () => { // Fire-and-forget actual send
                                    try {
                                        if (_exchangeCall == null || ct.IsCancellationRequested) {
                                            Console.WriteLine($"[{_identifier}] Send error: Call is null or task cancelled. Re-queueing.");
                                            _sendQueue.Enqueue(dataRecord); Interlocked.Increment(ref _availableCredits); return;
                                        }
                                        await _exchangeCall.RequestStream.WriteAsync(new UpstreamPayload { Record = dataRecord }, ct);
                                        if (!dataRecord.IsCheckpointBarrier) TaskManagerMetrics.RecordsSent.Add(1);
                                    } catch (OperationCanceledException) when (ct.IsCancellationRequested) {
                                        Console.WriteLine($"[{_identifier}] Send cancelled. Re-queueing.");
                                        _sendQueue.Enqueue(dataRecord); Interlocked.Increment(ref _availableCredits);
                                    } catch (Exception ex) {
                                        Console.WriteLine($"[{_identifier}] Send error: {ex.Message}. Re-queueing.");
                                        _sendQueue.Enqueue(dataRecord); Interlocked.Increment(ref _availableCredits);
                                        if (!_ctsInternal.IsCancellationRequested) _ctsInternal.Cancel(); // Signal overall failure
                                    } finally {
                                        _sendSignal.Set(); // Re-evaluate loop
                                    }
                                });
                            }

                            if (_availableCredits > 0 && !_sendQueue.IsEmpty) {
                                if (!processedItemInLock) _sendSignal.Set(); // Keep signaled if work can be done
                            } else {
                                _sendSignal.Reset(); // No credits or queue empty
                            }
                        }
                    }
                }
                catch (OperationCanceledException) { Console.WriteLine($"[{_identifier}] Send queue task cancelled."); }
                catch (Exception ex) { Console.WriteLine($"[{_identifier}] Critical error in send queue task: {ex.Message}"); }
                finally { Console.WriteLine($"[{_identifier}] Send queue task finished."); }
            }

            private async Task ProcessDownstreamMessagesAsync(CancellationToken ct)
            {
                Console.WriteLine($"[{_identifier}] Downstream message task started.");
                if (_exchangeCall == null) return;
                try {
                    await foreach (var downstreamPayload in _exchangeCall.ResponseStream.ReadAllAsync(ct)) {
                        if (downstreamPayload.PayloadCase == DownstreamPayload.PayloadOneofCase.Credits) {
                            int newCredits = downstreamPayload.Credits.CreditsGranted;
                            if (newCredits <=0) continue;
                            Interlocked.Add(ref _availableCredits, newCredits);
                            if (!_sendQueue.IsEmpty) _sendSignal.Set();
                        }
                    }
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { Console.WriteLine($"[{_identifier}] Downstream processing cancelled (server closed stream)."); }
                catch (OperationCanceledException) { Console.WriteLine($"[{_identifier}] Downstream processing task cancelled."); }
                catch (Exception ex) { Console.WriteLine($"[{_identifier}] Error processing downstream: {ex.Message}"); if (!_ctsInternal.IsCancellationRequested) _ctsInternal.Cancel(); }
                finally { Console.WriteLine($"[{_identifier}] Downstream processing loop ended."); if (!_ctsInternal.IsCancellationRequested) _ctsInternal.Cancel(); _sendSignal.Set(); }
            }

            public async Task CloseAsync() // Replaces CloseStreamAsync
            {
                Console.WriteLine($"[{_identifier}] CloseAsync called.");
                if (!_ctsInternal.IsCancellationRequested) _ctsInternal.Cancel();
                _sendSignal.Set();

                if (_exchangeCall?.RequestStream != null) {
                    try { await _exchangeCall.RequestStream.CompleteAsync(); }
                    catch (Exception ex) { Console.WriteLine($"[{_identifier}] Error completing request stream: {ex.Message}"); }
                }

                await Task.WhenAll(
                    _processSendQueueTask ?? Task.CompletedTask,
                    _processDownstreamMessagesTask ?? Task.CompletedTask
                ).WaitAsync(TimeSpan.FromSeconds(5)); // Wait for tasks with timeout

                _exchangeCall?.Dispose();
                if (_channel != null) await _channel.ShutdownAsync();

                _sendSignal.Dispose();
                _ctsInternal.Dispose();
                Console.WriteLine($"[{_identifier}] Closed. Credits: {_availableCredits}, Queue: {_sendQueue.Count}");
            }
        }


        // Old ExecuteTask method removed.

        // New method from prompt
        public async Task ExecuteFromDescriptor(
            TaskDeploymentDescriptor tdd,
            Dictionary<string, string> operatorProperties, // Already deserialized from TDD
            CancellationToken cancellationToken
            // _serializerRegistry, _checkpointingService, _taskManagerId, _stateStore are now fields of TaskExecutor
            )
    {
        // Register with ActiveTaskRegistry (conceptual 'main' branch logic)
        _activeTaskRegistry.RegisterTask(this, tdd.JobGraphJobId, tdd.JobVertexId, tdd.SubtaskIndex, tdd.TaskName);

        Console.WriteLine($"[{tdd.TaskName}] Attempting to execute task from TDD. Head Operator: {tdd.FullyQualifiedOperatorName}");
        var runtimeContext = new BasicRuntimeContext(
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
        ITypeSerializer<object>? inputDataSerializer = null; // Default to object for delegate
        Type? headOperatorInputType = null;

        if (!isHeadSource) // Only setup network input deserializer if head is not a source
        {
            if (string.IsNullOrEmpty(tdd.InputTypeName)) {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: InputTypeName is null or empty for a non-source task head."); return;
            }
            headOperatorInputType = Type.GetType(tdd.InputTypeName);
            if (headOperatorInputType == null) {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: Could not resolve head operator input type {tdd.InputTypeName}."); return;
            }

            if (string.IsNullOrEmpty(tdd.InputSerializerTypeName)) {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: InputSerializerTypeName is null or empty for a non-source task head."); return;
            }
            var headInputSerializerType = Type.GetType(tdd.InputSerializerTypeName);
            if (headInputSerializerType == null) {
                Console.WriteLine($"[{tdd.TaskName}] ERROR: Could not resolve head input serializer type {tdd.InputSerializerTypeName}."); return;
            }
            // TODO: Use _serializerRegistry from 'main' branch context if available for more robust serializer creation.
            // inputDataSerializer = _serializerRegistry.CreateSerializer(headInputSerializerType);
            inputDataSerializer = Activator.CreateInstance(headInputSerializerType) as ITypeSerializer<object>; // Fallback for now

            if (inputDataSerializer == null) {
                 Console.WriteLine($"[{tdd.TaskName}] ERROR: Input serializer for type {tdd.InputTypeName} could not be instantiated from {tdd.InputSerializerTypeName}.");
                 return;
            }
        }

        // State Restoration (Conceptual 'main' branch logic)
        if (tdd.IsRecovery)
        {
            Console.WriteLine($"[{tdd.TaskName}] Task is in RECOVERY mode for checkpoint ID {tdd.RecoveryCheckpointId}.");
            // In 'main', this would involve IStateSnapshotStore and IStateSnapshotReader

            // Example: Use _stateStore to get a reader for the recovery snapshot handle.
            // This is highly conceptual as the structure of state handles (one per task vs. per operator) is not defined.
            // IStateSnapshotReader? snapshotReader = _stateStore.GetReader(tdd.RecoverySnapshotHandle);
            // if (snapshotReader == null && !string.IsNullOrEmpty(tdd.RecoverySnapshotHandle))
            // {
            //    Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to get snapshot reader for handle {tdd.RecoverySnapshotHandle}. State restoration will be skipped.");
            // }

            foreach (var opInstance in allOperatorInstances)
            {
                if (opInstance is ICheckpointableOperator checkpointable)
                {
                    // If state is per operator, TDD would need to provide a map or list of handles.
                    // For now, assume a single handle applies or is adapted.
                    Console.WriteLine($"[{tdd.TaskName}] Attempting to restore state for {opInstance.GetType().Name} using recovery info from TDD.");
                    // await checkpointable.RestoreState(snapshotReader); // Pass the conceptual reader
                    // For placeholder:
                    if (string.IsNullOrEmpty(tdd.RecoverySnapshotHandle)) {
                        Console.WriteLine($"[{tdd.TaskName}] No RecoverySnapshotHandle provided in TDD for {opInstance.GetType().Name}. Skipping RestoreState.");
                    } else {
                        Console.WriteLine($"[{tdd.TaskName}] PLACEHOLDER: {opInstance.GetType().Name}.RestoreState() would be called with snapshot data from handle '{tdd.RecoverySnapshotHandle}'.");
                    }
                }
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
        OperatorBarrierHandler? barrierHandler = null; // Renamed from taskBarrierHandler
        if (!isHeadSource)
        {
            var expectedInputIds = tdd.Inputs
                .Select(inp => $"{inp.UpstreamJobVertexId}_{inp.UpstreamSubtaskIndex}")
                .ToList();

            Func<FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier, Task> onAlignedCallback =
                async (alignedBarrier) =>
            {
                Console.WriteLine($"[{tdd.TaskName}] All barriers for CP {alignedBarrier.CheckpointId} aligned. Starting snapshot and forwarding.");

                // 1. Snapshot State for all checkpointable operators in the chain
                // This is a simplified representation. Actual state handles/results would be collected.
                // And reported to _checkpointingService.AcknowledgeCheckpointAsync(...)
                bool allSnapshotsOk = true;
                foreach (var opInstance in allOperatorInstances)
                {
                    if (opInstance is ICheckpointableOperator checkpointable)
                    {
                        try
                        {
                            Console.WriteLine($"[{tdd.TaskName}] Snapshotting state for {opInstance.GetType().Name} for CP {alignedBarrier.CheckpointId}.");
                            // In real Flink, SnapshotStateContext would be passed, providing checkpointId, store etc.
                            // await checkpointable.SnapshotState(snapshotContext);
                            // For now, this is a placeholder.
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[{tdd.TaskName}] Error snapshotting {opInstance.GetType().Name} for CP {alignedBarrier.CheckpointId}: {ex.Message}");
                            allSnapshotsOk = false;
                            // TODO: Report snapshot failure to _checkpointingService
                            break;
                        }
                    }
                }

                if (!allSnapshotsOk) {
                     Console.WriteLine($"[{tdd.TaskName}] Snapshotting failed for one or more operators for CP {alignedBarrier.CheckpointId}. Checkpoint will likely be aborted by JM.");
                     return;
                }

                // TODO: Report overall snapshot success to _checkpointingService (e.g., with state handles)
                // For example: await _checkpointingService.AcknowledgeCheckpointAsync(
                //    _taskManagerId, tdd.JobGraphJobId, tdd.JobVertexId, tdd.SubtaskIndex,
                //    alignedBarrier.CheckpointId, "placeholder_state_handle_uri", null, 0, 0);


                // 2. Forward Barriers to all network outputs of the *last* operator in the chain
                if (allCollectors.LastOrDefault() is List<CreditAwareTaskOutput> outputSenders)
                {
                    Console.WriteLine($"[{tdd.TaskName}] Forwarding barrier CP {alignedBarrier.CheckpointId} to {outputSenders.Count} network outputs.");
                    var barrierDataRecord = new DataRecord {
                        IsCheckpointBarrier = true,
                        BarrierPayload = new Proto.Internal.CheckpointBarrier {
                            CheckpointId = alignedBarrier.CheckpointId,
                            Timestamp = alignedBarrier.Timestamp
                        },
                        // Target info will be set by NetworkedCollector itself based on its _outputInfo
                    };
                    foreach (var sender in outputSenders)
                    {
                        // NetworkedCollector's Collect method needs to be adapted to take a DataRecord directly or handle this.
                        // For now, assuming it can take a generic object and detect it's a barrier, or a specialized method.
                        // The existing NetworkedCollector.Collect(T record, bool isBarrier, ...) is better.
                        // We need to pass a "dummy" T record for the signature.
                        // This part shows the type complexity. For now, we'll assume `object` type for NetworkedCollector<T>
                        // or that the barrier itself can be sent as T if T is object.

                        // If NetworkedCollector is NetworkedCollector<object>
                        if (sender is NetworkedCollector<object> objSender) {
                             objSender.Collect(alignedBarrier as object ?? new object(), isBarrier: true, checkpointId: alignedBarrier.CheckpointId, checkpointTimestamp: alignedBarrier.Timestamp);
                        } else {
                             Console.WriteLine($"[{tdd.TaskName}] Cannot forward barrier: Output sender is not NetworkedCollector<object>.");
                             // This highlights the need for a common way to send barriers, perhaps a dedicated method on the collector interface.
                        }
                    }
                }
                 Console.WriteLine($"[{tdd.TaskName}] Finished onAlignedCallback for CP {alignedBarrier.CheckpointId}.");
            };

            // TODO: Define onAbortedCallback and onTimedOutCallback if needed by OperatorBarrierHandler from 'main'
            Action<long> onAbortedCallback = (cpId) => {
                 Console.WriteLine($"[{tdd.TaskName}] Checkpoint {cpId} aborted callback triggered.");
            };
            Action<long> onTimedOutCallback = (cpId) => {
                Console.WriteLine($"[{tdd.TaskName}] Checkpoint {cpId} timed out callback triggered.");
            };

            barrierHandler = new OperatorBarrierHandler(
                tdd.JobVertexId, // Using JobVertexId as part of key
                tdd.SubtaskIndex,
                expectedInputIds,
                onAlignedCallback
                // onAbortedCallback, // Pass if OBH constructor takes them
                // onTimedOutCallback // Pass if OBH constructor takes them
            );
            // barrierHandler.RegisterInputs(expectedInputIds); // If OBH has this method
            _operatorBarrierHandlers[$"{tdd.JobVertexId}_{tdd.SubtaskIndex}"] = barrierHandler; // Use a consistent key
            Console.WriteLine($"[{tdd.TaskName}] OperatorBarrierHandler initialized and registered for {tdd.JobVertexId}_{tdd.SubtaskIndex}. Expecting inputs: {string.Join(", ", expectedInputIds)}");


            if (inputDataSerializer != null)
            {
                ProcessRecordDelegate recordProcessor = async (targetJobVertexId, targetSubtaskIndex, payload) =>
                {
                    if (targetJobVertexId != tdd.JobVertexId || targetSubtaskIndex != tdd.SubtaskIndex) return;
                    try
                    {
                        var deserializedRecord = inputDataSerializer.Deserialize(payload);

                        // Keyed State Scoping (Conceptual 'main' branch logic)
                        if (tdd.InputKeyingInfo != null && tdd.InputKeyingInfo.Count > 0 && headOperatorInputType != null)
                        {
                            // Assume first keying info is relevant for the head operator.
                            // This is a simplification. Multiple inputs might have different keying.
                            var keyingInfoProto = tdd.InputKeyingInfo[0];
                            // In 'main', this would use _serializerRegistry to get KeySelector and Key Serializer
                            // Type keySelectorType = Type.GetType(keyingInfoProto.SerializedKeySelector);
                            // var keySelectorInstance = Activator.CreateInstance(keySelectorType) as IKeySelector<object, object>; // Highly simplified
                            // if (keySelectorInstance != null) {
                            //    object key = keySelectorInstance.GetKey(deserializedRecord);
                            //    (runtimeContext as BasicRuntimeContext)?.SetCurrentKey(key); // Assuming BasicRuntimeContext has SetCurrentKey
                            //    Console.WriteLine($"[{tdd.TaskName}] Set current key for state: {key}");
                            // } else {
                            //    Console.WriteLine($"[{tdd.TaskName}] WARNING: Could not create key selector for input keying.");
                            // }
                            Console.WriteLine($"[{tdd.TaskName}] PLACEHOLDER: Keyed state scoping logic would apply here using '{keyingInfoProto.SerializedKeySelector}'.");
                        }

                        if (allCollectors.Count > 0 && allCollectors[0] is ICollector<object> headCollector)
                        {
                            headCollector.Collect(deserializedRecord);
                        }
                        else if (allCollectors.Count == 0 && allOperatorInstances.Count > 0 && allOperatorInstances[0] is ISinkFunction<object> sinkFn)
                        {
                            sinkFn.Invoke(deserializedRecord, new SimpleSinkContext());
                        } else {
                             Console.WriteLine($"[{tdd.TaskName}] ERROR: Head collector not found or misconfigured for network input.");
                        }
                    }
                    catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] Error processing received record via head collector: {ex.Message} {ex.StackTrace}"); }
                };
                DataReceiverRegistry.RegisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex, recordProcessor);
                cancellationToken.Register(() => DataReceiverRegistry.UnregisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex));
            }
        }


        // --- Execution Logic ---
        if (isHeadSource && allOperatorInstances.Count > 0 && allOperatorInstances[0] is ISourceFunction<object> headSourceFunction) // Generic assumed as object for now
        {
            Console.WriteLine($"[{tdd.TaskName}] Preparing to run as a SOURCE task with head: {headSourceFunction.GetType().Name}.");

            // This is where RunSourceWithBarrierInjection would be called.
            // We need to determine the output type TOut for the source.
            // TDD.OutputTypeName should represent this if the source is the only operator,
            // or if it's chained, it's the input to the first chained op (or TDD.OutputTypeName if no chain).
            Type? sourceOutputType = null;
            if (tdd.ChainedOperatorInfo.Count > 0) {
                sourceOutputType = Type.GetType(tdd.ChainedOperatorInfo[0].InputTypeName);
            } else {
                sourceOutputType = Type.GetType(tdd.OutputTypeName);
            }

            if (sourceOutputType == null && tdd.Outputs.Count > 0) { // If it has outputs, its type must be known
                Console.WriteLine($"[{tdd.TaskName}] CRITICAL: Could not determine source output type for {headSourceFunction.GetType().Name}. Cannot run source task.");
                return;
            }

            List<Task> connectTasks = new List<Task>();
            if (allCollectors.Count > 0 && allCollectors.LastOrDefault() is List<CreditAwareTaskOutput> outputSendersForSource) {
                Console.WriteLine($"[{tdd.TaskName}] Source Task: Attempting to connect {outputSendersForSource.Count} CreditAwareTaskOutput(s).");
                foreach (var sender in outputSendersForSource) connectTasks.Add(sender.ConnectAsync(cancellationToken));
            }
            if (connectTasks.Count > 0) {
                try {
                    await Task.WhenAll(connectTasks);
                    Console.WriteLine($"[{tdd.TaskName}] Source Task: All CreditAwareTaskOutputs connected.");
                }
                catch (Exception ex) {
                    Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to connect one or more CreditAwareTaskOutputs for source: {ex.Message}. Task will not run.");
                    if (allCollectors.LastOrDefault() is List<CreditAwareTaskOutput> createdSenders) {
                        foreach (var sender in createdSenders) await sender.CloseAsync();
                    }
                    return;
                }
            }

            if (sourceOutputType != null)
            {
                // Dynamically invoke RunSourceWithBarrierInjection<TOut>
                MethodInfo? runMethod = typeof(TaskExecutor).GetMethod(nameof(RunSourceWithBarrierInjection), BindingFlags.NonPublic | BindingFlags.Instance);
                if (runMethod == null) throw new InvalidOperationException("RunSourceWithBarrierInjection method not found via reflection.");

                MethodInfo genericRunMethod = runMethod.MakeGenericMethod(sourceOutputType);

                // Prepare arguments for RunSourceWithBarrierInjection
                // The 'collectors' argument should be what the source directly outputs to.
                // If chained, allCollectors[0] is a ChainedCollector. If not chained, it's List<CreditAwareTaskOutput>.
                object sourceOutputCollectorOrSenders = allCollectors.Count > 0 ? allCollectors[0] : null!; // Can be null if source is a sink

                var sourceTaskWrapper = Activator.CreateInstance(
                    typeof(SourceTaskWrapper<>).MakeGenericType(sourceOutputType),
                    headSourceFunction, tdd.JobGraphJobId, tdd.JobVertexId, tdd.SubtaskIndex, tdd.TaskName);

                // _activeTaskRegistry.RegisterSourceTask(sourceTaskWrapper); // Conceptual from 'main'

                Console.WriteLine($"[{tdd.TaskName}] Invoking RunSourceWithBarrierInjection<{sourceOutputType.Name}> via reflection.");
                var taskRun = (Task?)genericRunMethod.Invoke(this, new object[] {
                    headSourceFunction,
                    sourceOutputCollectorOrSenders, // This needs to be List<INetworkedCollector> or adaptable
                    runtimeContext, // Pass runtimeContext
                    cancellationToken // Pass the main task cancellation token
                });
                if (taskRun != null) await taskRun;
                else Console.WriteLine($"[{tdd.TaskName}] ERROR: RunSourceWithBarrierInjection invocation returned null task.");
            }
            else // Source has no output type (e.g. a source that is also a sink and has no chained outputs)
            {
                 Console.WriteLine($"[{tdd.TaskName}] Source {headSourceFunction.GetType().Name} has no defined output type and no network outputs. Running directly.");
                 await Task.Run(() => headSourceFunction.Run(new SimpleSourceContext<object>(record => { /* Sink-like source, data dropped */ })), cancellationToken);
                 headSourceFunction.Cancel();
            }
        }
        else if (!isHeadSource)
        {
             Console.WriteLine($"[{tdd.TaskName}] Task (head: {allOperatorInstances[0].GetType().Name}) waiting for input via DataExchangeService.");
             try { await Task.Delay(Timeout.Infinite, cancellationToken); }
             catch (OperationCanceledException) { Console.WriteLine($"[{tdd.TaskName}] Task canceled."); }
        }
        else
        {
             Console.WriteLine($"[{tdd.TaskName}] Component type not supported or no specific execution path for head: {allOperatorInstances[0].GetType().Name}.");
        }

        // Cleanup
        try
        {
            for (int i = allOperatorInstances.Count - 1; i >= 0; i--) {
                if (allOperatorInstances[i] is IOperatorLifecycle lifecycle) lifecycle.Close();
            }

            List<Task> closeTasks = new List<Task>();
            if (allCollectors.Count > 0 && allCollectors.LastOrDefault() is List<CreditAwareTaskOutput> finalOutputSenders) {
                foreach (var sender in finalOutputSenders) closeTasks.Add(sender.CloseAsync());
            }
            if(closeTasks.Count > 0) await Task.WhenAll(closeTasks);

            // Cleanup from 'main' branch
            var barrierHandlerKey = $"{tdd.JobGraphJobId}_{tdd.JobVertexId}_{tdd.SubtaskIndex}"; // Consistent key
            if (_operatorBarrierHandlers.TryRemove(barrierHandlerKey, out var removedHandler))
            {
                removedHandler.Dispose();
                Console.WriteLine($"[{tdd.TaskName}] Disposed and removed OperatorBarrierHandler for {barrierHandlerKey}.");
            }
            _activeTaskRegistry.UnregisterTask(tdd.JobGraphJobId, tdd.JobVertexId, tdd.SubtaskIndex, tdd.TaskName);
            // Console.WriteLine($"[{tdd.TaskName}] PLACEHOLDER: ActiveTaskRegistry.UnregisterTask and barrierHandler.Dispose would be called."); // Old placeholder
        }
        finally
        {
             if (!isHeadSource && inputDataSerializer != null) DataReceiverRegistry.UnregisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex); // Check inputDataSerializer too
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
