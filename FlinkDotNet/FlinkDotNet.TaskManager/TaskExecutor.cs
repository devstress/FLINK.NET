#nullable enable
using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Operators; 
using FlinkDotNet.Core.Abstractions.Runtime;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Serializers; 
using FlinkDotNet.Core.Abstractions.Models.Checkpointing; 
using FlinkDotNet.Core.Abstractions.Models.State; 
using FlinkDotNet.Storage.FileSystem; 
using System.Text; 
using FlinkDotNet.Core.Abstractions.Execution; 
using FlinkDotNet.Proto.Internal;
using Grpc.Net.Client;
using Grpc.Core;
using Google.Protobuf;
using FlinkDotNet.TaskManager.Services;
using System.Collections.Generic;
using System.Collections.Concurrent; 
using System.Diagnostics.Metrics; 
using System.Globalization; 
using System.Threading.Channels; 
using FlinkDotNet.Core.Abstractions.Storage; 
using FlinkDotNet.Core.Abstractions.Collectors; // For ICollector (used by ChainedCollector)
using FlinkDotNet.Core.Abstractions.Functions; // For IKeySelector


namespace FlinkDotNet.TaskManager
{
    internal static class TaskManagerMetrics
    {
        private static readonly Meter TaskExecutorMeter = new Meter("FlinkDotNet.TaskManager.TaskExecutor", "1.0.0");
        internal static readonly Counter<long> RecordsSent = TaskExecutorMeter.CreateCounter<long>("flinkdotnet.taskmanager.records_sent", unit: "{records}", description: "Number of records sent by NetworkedCollector.");
    }

    public interface IOperatorBarrierHandler : IDisposable
    {
        Task HandleIncomingBarrier(long checkpointId, long checkpointTimestamp, string sourceInputId);
        void RegisterInputs(List<string> expectedInputIds); 
        void TriggerCheckpoint(long checkpointId, long timestamp); 
    }

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
            }

            bool allAligned = _expectedInputIds.Count > 0 && _expectedInputIds.All(id => checkpointBarriers.ContainsKey(id) && checkpointBarriers[id]);

            if (allAligned)
            {
                Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] All barriers aligned for CP ID: {checkpointId}. Triggering onAlignedCallback.");
                var barrierToCallback = new FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier
                {
                    CheckpointId = checkpointId,
                    Timestamp = checkpointTimestamp,
                };
                await _onAlignedCallback(barrierToCallback);
                _receivedBarriers.TryRemove(checkpointId, out _); 
            }
            else
            {
                 var receivedCount = checkpointBarriers.Count(kv => _expectedInputIds.Contains(kv.Key));
                 Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] CP ID: {checkpointId} not yet aligned. Received {receivedCount}/{_expectedInputIds.Count}. Waiting for others.");
            }
        }

        public void Dispose()
        {
            Console.WriteLine($"[{_jobVertexId}_{_subtaskIndex}] OperatorBarrierHandler disposed.");
            _receivedBarriers.Clear();
        }
    }
    
    // Interface for ActiveTaskRegistry to interact with SourceTaskWrapper regarding barrier injection
    public interface IBarrierInjectableSource
    {
        string JobId { get; }
        string JobVertexId { get; }
        int SubtaskIndex { get; }
        ChannelWriter<BarrierInjectionRequest> BarrierChannelWriter { get; }
        void ReportCompleted(); // To signal that the source has finished and the channel can be closed.
    }


    public class ActiveTaskRegistry {
        // Using IBarrierInjectableSource for sources that support barrier injection via channels.
        private readonly ConcurrentDictionary<string, IBarrierInjectableSource> _barrierInjectableSources = new();

        public void RegisterTask(TaskExecutor executor, string jobId, string vertexId, int subtaskIndex, string taskName) {
            Console.WriteLine($"[ActiveTaskRegistry] Task {taskName} ({jobId}/{vertexId}_{subtaskIndex}) registered with executor {executor.GetHashCode()}.");
        }
        public void UnregisterTask(string jobId, string vertexId, int subtaskIndex, string taskName) {
            Console.WriteLine($"[ActiveTaskRegistry] Task {taskName} ({jobId}/{vertexId}_{subtaskIndex}) unregistered.");
             // If it was a source task, ensure it's also unregistered from barrier injection
            UnregisterSource(jobId, vertexId, subtaskIndex);
        }

        public void RegisterSource(IBarrierInjectableSource sourceWrapper)
        {
            var key = $"{sourceWrapper.JobId}_{sourceWrapper.JobVertexId}_{sourceWrapper.SubtaskIndex}";
            _barrierInjectableSources[key] = sourceWrapper;
            Console.WriteLine($"[ActiveTaskRegistry] Registered IBarrierInjectableSource for {key}");
        }

        public void UnregisterSource(string jobId, string vertexId, int subtaskIndex)
        {
            var key = $"{jobId}_{vertexId}_{subtaskIndex}";
            if (_barrierInjectableSources.TryRemove(key, out var removedSource))
            {
                removedSource.ReportCompleted(); // Signal completion to close its barrier channel
                Console.WriteLine($"[ActiveTaskRegistry] Unregistered and reported completion for IBarrierInjectableSource {key}");
            }
        }
        
        public IBarrierInjectableSource? GetBarrierInjectableSource(string jobId, string vertexId, int subtaskIndex)
        {
            _barrierInjectableSources.TryGetValue($"{jobId}_{vertexId}_{subtaskIndex}", out var source);
            return source;
        }
    }

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
    
    // ChainedCollector from feature branch
    internal class ChainedCollector<T> : ICollector<T>
    {
        private readonly object _nextOperatorOrCollector; // Could be IMapOperator, ISinkFunction, or another ICollector (List<INetworkedCollector>)
        private readonly object? _outputCollectorForNext; // If nextOperator is itself chained, this is its output collector
        private readonly Type _expectedInputTypeForNext;


        public ChainedCollector(object nextOperatorOrCollector, object? outputCollectorForNext, Type expectedInputTypeForNext)
        {
            _nextOperatorOrCollector = nextOperatorOrCollector;
            _outputCollectorForNext = outputCollectorForNext; // This is used if _nextOperatorOrCollector is an IMapOperator that itself outputs to a ChainedCollector or List<INetworkedCollector>
            _expectedInputTypeForNext = expectedInputTypeForNext;
        }

        public void Collect(T record)
        {
            // This logic needs to be careful about types and how different operator types are handled.
            if (_nextOperatorOrCollector is IMapOperator<T, object> mapper)
            {
                var mappedOutput = mapper.Map(record);
                if (_outputCollectorForNext is ICollector<object> downstreamCollector)
                {
                    downstreamCollector.Collect(mappedOutput);
                }
                else if (_outputCollectorForNext is List<INetworkedCollector> networkedCollectors)
                {
                    foreach (var nc in networkedCollectors)
                    {
                        // Assuming INetworkedCollector has a compatible CollectObject or similar method
                        nc.CollectObject(mappedOutput, CancellationToken.None); // CancellationToken needs context
                    }
                }
            }
            else if (_nextOperatorOrCollector is ISinkFunction<T> sink)
            {
                sink.Invoke(record, new SimpleSinkContext());
            }
            else if (_nextOperatorOrCollector is ICollector<T> directCollector) // e.g. List<INetworkedCollector> directly
            {
                 directCollector.Collect(record);
            }
            else
            {
                // This case might indicate a type mismatch or an unhandled operator type in the chain.
                 Console.WriteLine($"ChainedCollector: Next operator type {_nextOperatorOrCollector.GetType().Name} not directly supported for Collect(T). Record dropped.");
            }
        }
    }


    public class TaskExecutor
    {
        private readonly ActiveTaskRegistry _activeTaskRegistry;
        private readonly TaskManagerCheckpointingServiceImpl _checkpointingService;
        private readonly SerializerRegistry _serializerRegistry;
        private readonly string _taskManagerId;
        private readonly IStateSnapshotStore _stateStore; 

        private readonly ConcurrentDictionary<string, IOperatorBarrierHandler> _operatorBarrierHandlers =
            new ConcurrentDictionary<string, IOperatorBarrierHandler>();

        public TaskExecutor(
            ActiveTaskRegistry activeTaskRegistry,
            TaskManagerCheckpointingServiceImpl checkpointingService,
            SerializerRegistry serializerRegistry,
            string taskManagerId,
            IStateSnapshotStore stateStore) 
        {
            _activeTaskRegistry = activeTaskRegistry ?? throw new ArgumentNullException(nameof(activeTaskRegistry));
            _checkpointingService = checkpointingService ?? throw new ArgumentNullException(nameof(checkpointingService));
            _serializerRegistry = serializerRegistry ?? throw new ArgumentNullException(nameof(serializerRegistry));
            _taskManagerId = taskManagerId ?? throw new ArgumentNullException(nameof(taskManagerId));
            _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore)); 
        }

        public IOperatorBarrierHandler? GetOperatorBarrierHandler(string jobVertexId, int subtaskIndex)
        {
            _operatorBarrierHandlers.TryGetValue($"{jobVertexId}_{subtaskIndex}", out var handler);
            return handler;
        }
        
        // SourceTaskWrapper (inner class from main)
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

        // Combined NetworkedCollector
        public interface INetworkedCollector
        {
            string TargetVertexId { get; }
            Task CollectObject(object record, CancellationToken cancellationToken);
            Task CloseAsync(); // Combined from CloseStreamAsync and CloseAsync
            Task ConnectAsync(CancellationToken externalCt); // From feature branch
        }

        public class NetworkedCollector<T, TKey> : INetworkedCollector
        {
            private readonly string _sourceJobVertexId;
            private readonly int _sourceSubtaskIndex;
            private readonly OperatorOutput _outputInfo;
            private readonly ITypeSerializer<T> _serializer;
            private readonly IKeySelector<T, TKey>? _keySelector;
            private readonly int? _downstreamParallelism;

            private GrpcChannel? _channel;
            private DataExchangeService.DataExchangeServiceClient? _client;
            private AsyncDuplexStreamingCall<UpstreamPayload, DownstreamPayload>? _exchangeCall;

            private int _availableCredits = 0;
            private readonly ConcurrentQueue<DataRecord> _sendQueue = new ConcurrentQueue<DataRecord>();
            private readonly ManualResetEventSlim _sendSignal = new ManualResetEventSlim(false);

            private Task? _processSendQueueTask;
            private Task? _processDownstreamMessagesTask;
            private readonly CancellationTokenSource _ctsInternal = new CancellationTokenSource();
            private CancellationToken _linkedCt = CancellationToken.None;
            private readonly string _identifier;
            
            public string TargetVertexId => _outputInfo.TargetVertexId;

            public NetworkedCollector(
                string sourceJobVertexId, int sourceSubtaskIndex,
                OperatorOutput outputInfo,
                ITypeSerializer<T> serializer,
                IKeySelector<T, TKey>? keySelector,
                int? downstreamParallelism)
            {
                _sourceJobVertexId = sourceJobVertexId;
                _sourceSubtaskIndex = sourceSubtaskIndex;
                _outputInfo = outputInfo;
                _serializer = serializer;
                _keySelector = keySelector;
                _downstreamParallelism = downstreamParallelism;
                _targetTmEndpoint = _outputInfo.TargetTaskEndpoint;
                _identifier = $"NetworkCollector-{_sourceJobVertexId}_{_sourceSubtaskIndex}->{_outputInfo.TargetVertexId}_{_outputInfo.TargetSpecificSubtaskIndex}";
                Console.WriteLine($"[{_identifier}] Created for target {_targetTmEndpoint}. Keyed: {_keySelector != null}");
            }

            public async Task ConnectAsync(CancellationToken externalCt)
            {
                _linkedCt = CancellationTokenSource.CreateLinkedTokenSource(externalCt, _ctsInternal.Token).Token;
                if (_exchangeCall != null) { Console.WriteLine($"[{_identifier}] ConnectAsync called but already connected or connecting."); return; }
                Console.WriteLine($"[{_identifier}] Connecting to {_targetTmEndpoint}...");
                try
                {
                    _channel = GrpcChannel.ForAddress(_targetTmEndpoint);
                    _client = new DataExchangeService.DataExchangeServiceClient(_channel);
                    _exchangeCall = _client.ExchangeData(cancellationToken: _linkedCt);
                    _processDownstreamMessagesTask = Task.Run(() => ProcessDownstreamMessagesAsync(_linkedCt), _linkedCt);
                    _processSendQueueTask = Task.Run(() => ProcessSendQueueAsync(_linkedCt), _linkedCt);
                    Console.WriteLine($"[{_identifier}] Successfully connected. Background tasks started.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{_identifier}] Connection to {_targetTmEndpoint} failed: {ex.Message}");
                    _exchangeCall = null; _channel = null;
                    if (!_ctsInternal.IsCancellationRequested) _ctsInternal.Cancel();
                    throw;
                }
            }
            
            public async Task CollectObject(object record, CancellationToken cancellationToken)
            {
                // Handle if record is CheckpointBarrier first
                if (record is FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier actualBarrier)
                {
                    CollectInternal(default(T)!, true, actualBarrier.CheckpointId, actualBarrier.Timestamp);
                }
                else if (record is T typedRecord) // Ensure it's of type T for serialization
                {
                    CollectInternal(typedRecord, false, 0, 0);
                }
                else 
                {
                     Console.WriteLine($"[{_identifier}] ERROR: Record type mismatch in CollectObject. Expected {typeof(T).Name} or CheckpointBarrier, got {record.GetType().Name}. Record dropped.");
                }
                await Task.CompletedTask; // To match INetworkedCollector signature if it becomes async
            }


            private void CollectInternal(T record, bool isBarrier, long checkpointId, long checkpointTimestamp)
            {
                if (_ctsInternal.IsCancellationRequested) return;

                DataRecord dataRecord;
                if (isBarrier)
                {
                    dataRecord = new DataRecord {
                        TargetJobVertexId = _outputInfo.TargetVertexId,
                        TargetSubtaskIndex = _outputInfo.TargetSpecificSubtaskIndex, // Barriers go to all specific targets
                        SourceJobVertexId = _sourceJobVertexId,
                        SourceSubtaskIndex = _sourceSubtaskIndex,
                        IsCheckpointBarrier = true,
                        BarrierPayload = new Proto.Internal.CheckpointBarrier { CheckpointId = checkpointId, CheckpointTimestamp = checkpointTimestamp },
                        Payload = ByteString.Empty 
                    };
                }
                else
                {
                    byte[] payloadBytes = _serializer.Serialize(record);
                    dataRecord = new DataRecord {
                        TargetJobVertexId = _outputInfo.TargetVertexId,
                        TargetSubtaskIndex = _outputInfo.TargetSpecificSubtaskIndex, // Default, will be overridden by keying if applicable
                        SourceJobVertexId = _sourceJobVertexId,
                        SourceSubtaskIndex = _sourceSubtaskIndex,
                        IsCheckpointBarrier = false,
                        Payload = ByteString.CopyFrom(payloadBytes)
                    };

                    if (_keySelector != null && _downstreamParallelism.HasValue && _downstreamParallelism.Value > 0)
                    {
                        TKey key = _keySelector.GetKey(record);
                        int hashCode = key == null ? 0 : key.GetHashCode();
                        int targetSubtask = (hashCode & int.MaxValue) % _downstreamParallelism.Value;
                        dataRecord.TargetSubtaskIndex = targetSubtask;
                    }
                }
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

                        DataRecord? dataRecordToSend = null;
                        lock (_sendSignal) 
                        {
                            if (_availableCredits > 0 && _sendQueue.TryDequeue(out dataRecordToSend))
                            {
                                Interlocked.Decrement(ref _availableCredits);
                            }
                            
                            if (_availableCredits > 0 && !_sendQueue.IsEmpty) {
                                _sendSignal.Set(); 
                            } else {
                                _sendSignal.Reset(); 
                            }
                        }

                        if (dataRecordToSend != null) {
                             try {
                                if (_exchangeCall == null || ct.IsCancellationRequested) {
                                    Console.WriteLine($"[{_identifier}] Send error: Call is null or task cancelled. Re-queueing.");
                                    _sendQueue.Enqueue(dataRecordToSend); Interlocked.Increment(ref _availableCredits); continue;
                                }
                                await _exchangeCall.RequestStream.WriteAsync(new UpstreamPayload { Record = dataRecordToSend }, ct);
                                if (!dataRecordToSend.IsCheckpointBarrier) TaskManagerMetrics.RecordsSent.Add(1);
                            } catch (OperationCanceledException) when (ct.IsCancellationRequested) {
                                Console.WriteLine($"[{_identifier}] Send cancelled. Re-queueing.");
                                _sendQueue.Enqueue(dataRecordToSend); Interlocked.Increment(ref _availableCredits);
                            } catch (Exception ex) {
                                Console.WriteLine($"[{_identifier}] Send error: {ex.Message}. Re-queueing.");
                                _sendQueue.Enqueue(dataRecordToSend); Interlocked.Increment(ref _availableCredits);
                                if (!_ctsInternal.IsCancellationRequested) _ctsInternal.Cancel(); 
                            } finally {
                                if (_availableCredits > 0 && !_sendQueue.IsEmpty) _sendSignal.Set(); // Re-evaluate loop
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

            public async Task CloseAsync() 
            {
                Console.WriteLine($"[{_identifier}] CloseAsync called.");
                if (!_ctsInternal.IsCancellationRequested) _ctsInternal.Cancel();
                _sendSignal.Set(); 

                if (_exchangeCall?.RequestStream != null) {
                    try { await _exchangeCall.RequestStream.CompleteAsync(); }
                    catch (Exception ex) { Console.WriteLine($"[{_identifier}] Error completing request stream: {ex.Message}"); }
                }
                
                var tasksToWait = new List<Task>();
                if (_processSendQueueTask != null) tasksToWait.Add(_processSendQueueTask);
                if (_processDownstreamMessagesTask != null) tasksToWait.Add(_processDownstreamMessagesTask);

                if (tasksToWait.Any()) {
                    await Task.WhenAll(tasksToWait).WaitAsync(TimeSpan.FromSeconds(5)); 
                }

                _exchangeCall?.Dispose();
                if (_channel != null) await _channel.ShutdownAsync();
                _sendSignal.Dispose();
                _ctsInternal.Dispose();
                Console.WriteLine($"[{_identifier}] Closed. Credits: {_availableCredits}, Queue: {_sendQueue.Count}");
            }
        }

        public async Task ExecuteFromDescriptor(
            TaskDeploymentDescriptor tdd,
            Dictionary<string, string> operatorProperties, 
            CancellationToken cancellationToken)
        {
            _activeTaskRegistry.RegisterTask(this, tdd.JobGraphJobId, tdd.JobVertexId, tdd.SubtaskIndex, tdd.TaskName);

            Console.WriteLine($"[{tdd.TaskName}] Attempting to execute task from TDD. Head Operator: {tdd.FullyQualifiedOperatorName}");
            var runtimeContext = new BasicRuntimeContext(
                jobName: tdd.JobGraphJobId,
                taskName: tdd.TaskName,
                numberOfParallelSubtasks: tdd.Parallelism, // Using TDD parallelism
                indexOfThisSubtask: tdd.SubtaskIndex
            );

            var allOperatorInstances = new List<object>();
            var allCollectors = new List<object>(); // Holds ChainedCollector or List<INetworkedCollector>

            object? headOperatorInstance = null;
            Type? headOperatorType = Type.GetType(tdd.FullyQualifiedOperatorName);

            if (headOperatorType == null) { Console.WriteLine($"[{tdd.TaskName}] ERROR: Head operator type '{tdd.FullyQualifiedOperatorName}' not found."); return; }
            try {
                headOperatorInstance = Activator.CreateInstance(headOperatorType);
                if (headOperatorInstance == null) throw new InvalidOperationException("Activator.CreateInstance returned null");
                allOperatorInstances.Add(headOperatorInstance);
            } catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to instantiate head operator {tdd.FullyQualifiedOperatorName}: {ex.Message}"); return; }

            bool isHeadSource = tdd.Inputs.Count == 0;
            
            if (tdd.ChainedOperatorInfo != null && tdd.ChainedOperatorInfo.Count > 0) {
                foreach (var chainedOpProto in tdd.ChainedOperatorInfo) {
                    Type? chainedOpType = Type.GetType(chainedOpProto.FullyQualifiedOperatorName);
                    if (chainedOpType == null) { Console.WriteLine($"[{tdd.TaskName}] ERROR: Chained operator type '{chainedOpProto.FullyQualifiedOperatorName}' not found."); return; }
                    try {
                        object chainedOpInstance = Activator.CreateInstance(chainedOpType);
                        if (chainedOpInstance == null) throw new InvalidOperationException("Activator.CreateInstance returned null for chained operator.");
                        allOperatorInstances.Add(chainedOpInstance);
                    } catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to instantiate chained operator {chainedOpProto.FullyQualifiedOperatorName}: {ex.Message}"); return; }
                }
            }

            foreach (var opInstance in allOperatorInstances) {
                if (opInstance is IOperatorLifecycle lifecycle) {
                    try { lifecycle.Open(runtimeContext); }
                    catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to open operator {opInstance.GetType().Name}: {ex.Message}"); return; }
                }
            }
            
            // Initialize allCollectors placeholders
            for (int i = 0; i < allOperatorInstances.Count; i++) allCollectors.Add(null!);

            // Setup NetworkedCollectors for the last operator in the chain (or the head if no chain)
            if (tdd.Outputs.Count > 0)
            {
                var actualNetworkedCollectors = new List<INetworkedCollector>();
                Type? taskOutputOverallType = Type.GetType(tdd.OutputTypeName); // Overall output type of the task/chain
                if (taskOutputOverallType == null) {
                     Console.WriteLine($"[{tdd.TaskName}] ERROR: Task OutputTypeName '{tdd.OutputTypeName}' could not be resolved."); return;
                }

                foreach (var outputDesc in tdd.Outputs)
                {
                    ITypeSerializer specificSerializer = _serializerRegistry.GetSerializer(taskOutputOverallType);
                    IKeySelector<object,object>? keySelectorInstance = null; // Use object,object for IKeySelector if type not fully known here
                    int? keyingDownstreamParallelism = null;
                    Type keyTypeForCollector = typeof(object); // Default TKey for NetworkedCollector to object if not keyed

                    var keyingInfo = tdd.OutputKeyingInfo.FirstOrDefault(k => k.TargetJobVertexId == outputDesc.TargetVertexId);
                    if (keyingInfo != null) { // Keyed Output
                        Type? keySelectorActualType = Type.GetType(keyingInfo.KeySelectorTypeName);
                        Type? keyActualType = Type.GetType(keyingInfo.KeyTypeName);
                        if (keySelectorActualType != null && keyActualType != null) {
                            try {
                                byte[] serializedKeySelectorBytes = Convert.FromBase64String(keyingInfo.SerializedKeySelector);
                                ITypeSerializer keySelectorSerializer = _serializerRegistry.GetSerializer(keySelectorActualType);
                                keySelectorInstance = keySelectorSerializer.Deserialize(serializedKeySelectorBytes) as IKeySelector<object,object>;
                                keyingDownstreamParallelism = keyingInfo.DownstreamParallelism;
                                keyTypeForCollector = keyActualType;
                            } catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] ERROR: Failed to deserialize KeySelector for output {outputDesc.TargetVertexId}. Ex: {ex.Message}"); }
                        } else { Console.WriteLine($"[{tdd.TaskName}] ERROR: Could not load KeySelectorType or KeyType for output {outputDesc.TargetVertexId}."); }
                    }
                    
                    // Create specific NetworkedCollector<T, TKey>
                    Type genericCollectorType = typeof(NetworkedCollector<,>);
                    Type specificCollectorType = genericCollectorType.MakeGenericType(taskOutputOverallType, keyTypeForCollector);
                    
                    INetworkedCollector collector = (INetworkedCollector)Activator.CreateInstance(
                        specificCollectorType,
                        tdd.JobVertexId, tdd.SubtaskIndex, outputDesc, specificSerializer, keySelectorInstance, keyingDownstreamParallelism)!;
                    actualNetworkedCollectors.Add(collector);
                }
                allCollectors[allOperatorInstances.Count - 1] = actualNetworkedCollectors;
            }

            // Populate ChainedCollectors by iterating backwards
            for (int i = allOperatorInstances.Count - 2; i >= 0; i--)
            {
                object currentOperator = allOperatorInstances[i];
                object nextOperator = allOperatorInstances[i+1];
                object? collectorForNextOperatorOutput = allCollectors[i+1];
                Type? outputTypeOfCurrentOperator = null; Type? inputTypeOfNextOperator = null;

                if (i == 0) { // Head operator
                    outputTypeOfCurrentOperator = (tdd.ChainedOperatorInfo.Count > 0) ? Type.GetType(tdd.ChainedOperatorInfo[0].InputTypeName) : Type.GetType(tdd.OutputTypeName);
                } else { // Chained operator
                    outputTypeOfCurrentOperator = Type.GetType(tdd.ChainedOperatorInfo[i-1].OutputTypeName);
                }
                inputTypeOfNextOperator = Type.GetType(tdd.ChainedOperatorInfo[i].InputTypeName);

                if (outputTypeOfCurrentOperator == null || inputTypeOfNextOperator == null) { Console.WriteLine($"[{tdd.TaskName}] Error resolving types for ChainedCollector between {currentOperator.GetType().Name} and {nextOperator.GetType().Name}."); continue; }
                
                Type chainedCollectorGenericType = typeof(ChainedCollector<>);
                Type chainedCollectorTyped = chainedCollectorGenericType.MakeGenericType(outputTypeOfCurrentOperator);
                object chainedCollectorInstance = Activator.CreateInstance(chainedCollectorTyped, nextOperator, collectorForNextOperatorOutput, inputTypeOfNextOperator)!;
                allCollectors[i] = chainedCollectorInstance;
            }

            ITypeSerializer<object>? inputDataSerializer = null;
            if (!isHeadSource) {
                if (string.IsNullOrEmpty(tdd.InputTypeName) || string.IsNullOrEmpty(tdd.InputSerializerTypeName)) { Console.WriteLine($"[{tdd.TaskName}] ERROR: Input type or serializer not defined for non-source task head."); return; }
                Type? headInputType = Type.GetType(tdd.InputTypeName);
                Type? headInputSerializerDefType = Type.GetType(tdd.InputSerializerTypeName);
                if (headInputType == null || headInputSerializerDefType == null) { Console.WriteLine($"[{tdd.TaskName}] ERROR: Could not resolve head input type/serializer."); return; }
                inputDataSerializer = _serializerRegistry.GetSerializer(headInputType) as ITypeSerializer<object> ?? Activator.CreateInstance(headInputSerializerDefType) as ITypeSerializer<object>;
                if (inputDataSerializer == null) { Console.WriteLine($"[{tdd.TaskName}] ERROR: Input serializer for type {tdd.InputTypeName} could not be instantiated."); return; }
            }

            if (tdd.IsRecovery) {
                Console.WriteLine($"[{tdd.TaskName}] Task is in RECOVERY mode for CP ID {tdd.RecoveryCheckpointId}.");
                if (string.IsNullOrEmpty(tdd.RecoverySnapshotHandle)) { Console.WriteLine($"[{tdd.TaskName}] RECOVERY WARNING: Missing RecoverySnapshotHandle."); }
                else {
                    foreach (var opInstance in allOperatorInstances) {
                        if (opInstance is ICheckpointableOperator checkpointable) {
                            Console.WriteLine($"[{tdd.TaskName}] Restoring state for {opInstance.GetType().Name}.");
                            try {
                                await checkpointable.RestoreState(new OperatorStateSnapshot(tdd.RecoverySnapshotHandle, 0L)); // Simplified
                            } catch (Exception ex) {Console.WriteLine($"[{tdd.TaskName}] Error restoring state for {opInstance.GetType().Name}: {ex}");}
                        }
                    }
                }
            }
            
            IOperatorBarrierHandler? barrierHandlerForInputs = null;
            if (!isHeadSource) {
                var expectedInputIds = tdd.Inputs.Select(inp => $"{inp.UpstreamJobVertexId}_{inp.UpstreamSubtaskIndex}").ToList();
                Func<FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier, Task> onAlignedCallback = async (alignedBarrier) => {
                    Console.WriteLine($"[{tdd.TaskName}] All barriers for CP {alignedBarrier.CheckpointId} aligned. Starting snapshot and forwarding.");
                    bool allSnapshotsOk = true;
                    var snapshotResults = new List<OperatorStateSnapshot>();

                    foreach (var opInstance in allOperatorInstances) {
                        if (opInstance is ICheckpointableOperator checkpointable) {
                            try {
                                var snapshotResult = await checkpointable.SnapshotState(alignedBarrier.CheckpointId, alignedBarrier.Timestamp);
                                if(snapshotResult != null) snapshotResults.Add(snapshotResult);
                                else { allSnapshotsOk = false; Console.WriteLine($"[{tdd.TaskName}] Snapshot for {opInstance.GetType().Name} returned null."); }
                            } catch (Exception ex) {
                                Console.WriteLine($"[{tdd.TaskName}] Error snapshotting {opInstance.GetType().Name}: {ex.Message}");
                                allSnapshotsOk = false; break;
                            }
                        }
                    }

                    if (allSnapshotsOk) {
                         // Simplified: report all snapshots to checkpointing service
                        foreach(var sr in snapshotResults) {
                             await _checkpointingService.ReportOperatorSnapshotComplete(
                                tdd.JobGraphJobId, alignedBarrier.CheckpointId, tdd.JobVertexId, tdd.SubtaskIndex, // This should be specific operator's ID if different
                                sr, 50 /* placeholder duration */);
                        }
                        Console.WriteLine($"[{tdd.TaskName}] All snapshots reported for CP {alignedBarrier.CheckpointId}.");
                    } else {
                         Console.WriteLine($"[{tdd.TaskName}] Snapshotting failed for CP {alignedBarrier.CheckpointId}.");
                         // TODO: Report checkpoint failure
                         return;
                    }

                    if (allCollectors.LastOrDefault() is List<INetworkedCollector> outputSenders) {
                        Console.WriteLine($"[{tdd.TaskName}] Forwarding barrier CP {alignedBarrier.CheckpointId} to {outputSenders.Count} network outputs.");
                        foreach (var sender in outputSenders) {
                            await sender.CollectObject(alignedBarrier, cancellationToken);
                        }
                    }
                    Console.WriteLine($"[{tdd.TaskName}] Finished onAlignedCallback for CP {alignedBarrier.CheckpointId}.");
                };
                barrierHandlerForInputs = new OperatorBarrierHandler(tdd.JobVertexId, tdd.SubtaskIndex, expectedInputIds, onAlignedCallback);
                _operatorBarrierHandlers[$"{tdd.JobVertexId}_{tdd.SubtaskIndex}"] = barrierHandlerForInputs;
                
                if (inputDataSerializer != null) {
                    ProcessRecordDelegate recordProcessor = async (targetJobVertexId, targetSubtaskIndex, payload) => {
                        if (targetJobVertexId != tdd.JobVertexId || targetSubtaskIndex != tdd.SubtaskIndex) return;
                        try {
                            var deserializedRecord = inputDataSerializer.Deserialize(payload);
                            object? currentKeyForState = null;
                            // Keyed State Scoping from main
                            if (tdd.InputKeyingInfo != null && tdd.InputKeyingInfo.Any() && headOperatorInstance is IKeyedOperator) {
                                var inputKeyingConfig = tdd.InputKeyingInfo.First(); // Assuming one input keying for head
                                if (!string.IsNullOrEmpty(inputKeyingConfig.SerializedKeySelector) && !string.IsNullOrEmpty(inputKeyingConfig.KeySelectorTypeName)) {
                                    try {
                                        Type? keySelectorType = Type.GetType(inputKeyingConfig.KeySelectorTypeName);
                                        if (keySelectorType != null) {
                                            ITypeSerializer keySelectorSerializer = _serializerRegistry.GetSerializer(keySelectorType);
                                            object keySelectorInstance = keySelectorSerializer.Deserialize(Convert.FromBase64String(inputKeyingConfig.SerializedKeySelector))!;
                                            MethodInfo? getKeyMethod = keySelectorType.GetMethod("GetKey");
                                            currentKeyForState = getKeyMethod?.Invoke(keySelectorInstance, new object[] { deserializedRecord! });
                                        }
                                    } catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] ERROR using input KeySelector: {ex.Message}"); }
                                }
                            }
                            if (runtimeContext is BasicRuntimeContext brc) { brc.SetCurrentKey(currentKeyForState); }

                            if (allCollectors.Count > 0 && allCollectors[0] is ICollector<object> headCollector) {
                                headCollector.Collect(deserializedRecord);
                            } else if (allCollectors.Count == 0 && headOperatorInstance is ISinkFunction<object> sinkFn) {
                                sinkFn.Invoke(deserializedRecord, new SimpleSinkContext());
                            } else { Console.WriteLine($"[{tdd.TaskName}] ERROR: Head collector/sink not found or misconfigured."); }
                             if (runtimeContext is BasicRuntimeContext brcAfter) { brcAfter.SetCurrentKey(null); }
                        } catch (Exception ex) { Console.WriteLine($"[{tdd.TaskName}] Error processing received record: {ex.Message} {ex.StackTrace}"); }
                    };
                    DataReceiverRegistry.RegisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex, recordProcessor);
                    cancellationToken.Register(() => DataReceiverRegistry.UnregisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex));
                }
            }

            // --- Execution Logic ---
            if (isHeadSource && headOperatorInstance is ISourceFunction<object> headSourceFunction) {
                Console.WriteLine($"[{tdd.TaskName}] Running as a SOURCE task: {headSourceFunction.GetType().Name}");
                var sourceTaskWrapper = new SourceTaskWrapper(
                    tdd.JobGraphJobId, tdd.JobVertexId, tdd.SubtaskIndex,
                    () => _activeTaskRegistry.UnregisterSource(tdd.JobGraphJobId, tdd.JobVertexId, tdd.SubtaskIndex)
                );
                _activeTaskRegistry.RegisterSource(sourceTaskWrapper);

                List<INetworkedCollector> networkedOutputs = new List<INetworkedCollector>();
                if(allCollectors.Count > 0 && allCollectors.LastOrDefault() is List<INetworkedCollector> nos) {
                    networkedOutputs = nos;
                } else if (allCollectors.Count > 0 && allCollectors.LastOrDefault() is INetworkedCollector singleNc) {
                    // This case should ideally not happen if the setup logic for allCollectors is correct for sources
                    networkedOutputs.Add(singleNc);
                }

                // Connect all networked outputs
                var connectTasks = networkedOutputs.Select(sender => sender.ConnectAsync(cancellationToken)).ToList();
                try { await Task.WhenAll(connectTasks); }
                catch(Exception ex) { Console.WriteLine($"[{tdd.TaskName}] ERROR connecting source outputs: {ex.Message}"); return; }


                Type? sourceOutputType = Type.GetType(tdd.OutputTypeName); // Output type of the source itself
                if (sourceOutputType == null && networkedOutputs.Any()) { Console.WriteLine($"[{tdd.TaskName}] CRITICAL: Could not determine source output type for {headSourceFunction.GetType().Name}."); return; }

                if (sourceOutputType != null) {
                     MethodInfo? runMethod = typeof(TaskExecutor)
                        .GetMethod(nameof(RunSourceWithBarrierInjection), BindingFlags.NonPublic | BindingFlags.Instance)
                        ?.MakeGenericMethod(sourceOutputType);
                    if (runMethod == null) throw new InvalidOperationException("RunSourceWithBarrierInjection method not found.");
                    
                    await (Task)runMethod.Invoke(this, new object[] {
                        headSourceFunction,
                        networkedOutputs, // Pass List<INetworkedCollector>
                        sourceTaskWrapper.BarrierChannelReader,
                        cancellationToken,
                        new Action(headSourceFunction.Cancel)
                    })!;
                } else { // Source with no defined output type or no network outputs
                     await Task.Run(() => headSourceFunction.Run(new SimpleSourceContext<object>(record => {})), cancellationToken);
                     headSourceFunction.Cancel();
                }

            } else if (!isHeadSource) { // Operator or Sink
                Console.WriteLine($"[{tdd.TaskName}] Task (head: {headOperatorInstance?.GetType().Name}) waiting for input.");
                try { await Task.Delay(Timeout.Infinite, cancellationToken); }
                catch (OperationCanceledException) { Console.WriteLine($"[{tdd.TaskName}] Task canceled."); }
            } else {
                 Console.WriteLine($"[{tdd.TaskName}] Component type not supported or no specific execution path for head: {headOperatorInstance?.GetType().Name}.");
            }

            // Cleanup
            try {
                foreach (var opInstance in allOperatorInstances) { if (opInstance is IOperatorLifecycle lifecycle) lifecycle.Close(); }
                
                if (allCollectors.LastOrDefault() is List<INetworkedCollector> finalOutputSenders) {
                    await Task.WhenAll(finalOutputSenders.Select(s => s.CloseAsync()));
                }
                var barrierHandlerKeyToRemove = $"{tdd.JobVertexId}_{tdd.SubtaskIndex}";
                if (_operatorBarrierHandlers.TryRemove(barrierHandlerKeyToRemove, out var removedHandler)) {
                    removedHandler.Dispose();
                }
                 _activeTaskRegistry.UnregisterTask(tdd.JobGraphJobId, tdd.JobVertexId, tdd.SubtaskIndex, tdd.TaskName);
            } finally {
                 if (!isHeadSource && inputDataSerializer != null) DataReceiverRegistry.UnregisterReceiver(tdd.JobVertexId, tdd.SubtaskIndex);
                 Console.WriteLine($"[{tdd.TaskName}] Execution finished and cleaned up.");
            }
        }
        
        // RunSourceWithBarrierInjection from main
        private async Task RunSourceWithBarrierInjection<TOut>(
            ISourceFunction<TOut> sourceFunction,
            List<INetworkedCollector> collectors, 
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
                        var collectTasks = new List<Task>();
                        foreach (INetworkedCollector collector in collectors)
                        {
                            collectTasks.Add(collector.CollectObject(record!, taskOverallCancellation));
                        }
                        await Task.WhenAll(collectTasks);
                    });
                    sourceFunction.Run(sourceContext);
                }
                finally { sourceRunCompleted = true; }
            }, sourceCts.Token);

            try {
                while (!sourceRunCompleted && !taskOverallCancellation.IsCancellationRequested) {
                    BarrierInjectionRequest? barrierRequest = null;
                    try {
                        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
                        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(taskOverallCancellation, timeoutCts.Token);
                        barrierRequest = await barrierRequests.ReadAsync(linkedCts.Token);
                    }
                    catch (OperationCanceledException) { /* Expected */ }
                    catch (ChannelClosedException) { break; }

                    if (barrierRequest != null) {
                        Console.WriteLine($"TaskExecutor: Injecting barrier {barrierRequest.CheckpointId} for {sourceFunction.GetType().Name}");
                        var barrier = new FlinkDotNet.Core.Abstractions.Models.Checkpointing.CheckpointBarrier(
                            barrierRequest.CheckpointId, barrierRequest.CheckpointTimestamp); // Removed Options = null
                        var barrierTasks = new List<Task>();
                        foreach (INetworkedCollector collector in collectors) {
                            barrierTasks.Add(collector.CollectObject(barrier, taskOverallCancellation));
                        }
                        await Task.WhenAll(barrierTasks);
                    }
                    if (sourceRunTask.IsCompleted || sourceRunTask.IsFaulted || sourceRunTask.IsCanceled) break;
                }
            }
            finally {
                if (!sourceCts.IsCancellationRequested) sourceCts.Cancel();
                cancelSourceFunctionAction();
                try { await sourceRunTask; } catch { /* Ignored as source cancellation might throw */ }
            }
        }
    } 
    
    // Dummy IKeyedOperator for testing compilation
    public interface IKeyedOperator {}

    public class SimpleStringToUpperMapOperator : IMapOperator<object, object>, IOperatorLifecycle, ICheckpointableOperator 
    {
        private string _taskName = nameof(SimpleStringToUpperMapOperator);
        private IRuntimeContext? _context; 

        public void Open(IRuntimeContext context) {
            _taskName = context.TaskName; _context = context; 
            Console.WriteLine($"[{_taskName}] SimpleStringToUpperMapOperator opened."); 
        }
        public object Map(object record) => record.ToString()?.ToUpper() ?? "";
        public void Close() { Console.WriteLine($"[{_taskName}] SimpleStringToUpperMapOperator closed."); }

        public async Task<OperatorStateSnapshot> SnapshotState(long checkpointId, long checkpointTimestamp) {
            Console.WriteLine($"[{_taskName}] SnapshotState called for CP ID: {checkpointId}, Timestamp: {checkpointTimestamp}.");
            var snapshotStore = new FileSystemSnapshotStore("file:///tmp/flinkdotnet/snapshots"); // Example path
            var dummyStateData = System.Text.Encoding.UTF8.GetBytes($"State for operator {_taskName} (Instance: {_context?.TaskName}) at checkpoint {checkpointId} on {DateTime.UtcNow:o}");
            string jobGraphJobId = _context!.JobName; // Non-null asserted
            string operatorInstanceId = _context!.TaskName; // Non-null asserted
            
            var snapshotHandleRecord = await snapshotStore.StoreSnapshot(
                jobGraphJobId, checkpointId, Program.TaskManagerId, operatorInstanceId, dummyStateData);

            var snapshot = new OperatorStateSnapshot(snapshotHandleRecord.Value, dummyStateData.Length) {
                Metadata = new Dictionary<string, string> {
                    { "OperatorType", _taskName }, { "OperatorInstanceId", operatorInstanceId },
                    { "CheckpointTime", DateTime.UtcNow.ToString("o") }, { "TaskManagerId", Program.TaskManagerId }
                }
            };
            Console.WriteLine($"[{_taskName}] Operator {operatorInstanceId} stored snapshot. Handle: {snapshot.StateHandle}, Size: {snapshot.StateSize}");
            return snapshot;
        }
        
        public Task RestoreState(OperatorStateSnapshot snapshot) { // Added RestoreState
             Console.WriteLine($"[{_taskName}] RestoreState called. Snapshot Handle: {snapshot.StateHandle}");
             // In a real scenario, you would use snapshotStore.ReadSnapshot and deserialize.
             return Task.CompletedTask;
        }
    }
}
#nullable disable
