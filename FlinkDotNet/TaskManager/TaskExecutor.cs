using System;
using System.Collections.Concurrent; // For CheckpointBarrierAligner's dictionary
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Globalization;
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Functions;
using FlinkDotNet.Core.Abstractions.Operators;
using FlinkDotNet.Core.Abstractions.Sinks;
using FlinkDotNet.Core.Abstractions.Sources;
using FlinkDotNet.Core.Abstractions.Serializers;
using FlinkDotNet.Core.Abstractions.Runtime;
using FlinkDotNet.Core.Abstractions.Collectors;
using FlinkDotNet.Core.Abstractions.Storage; // For state store interfaces
using FlinkDotNet.Core.Networking; // For INetworkBufferPool
using FlinkDotNet.TaskManager.Internal; // For KeySelectorActivator
using FlinkDotNet.Proto.Internal;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;


// Minimal necessary interface definitions for this TaskExecutor file to be self-contained for the demo.
namespace FlinkDotNet.Core.Abstractions.Operators
{
    public interface IFilterOperator<T> { bool Filter(T value); }
    public interface IMapOperator<in TIn, out TOut> { TOut Map(TIn value); }
    public interface IFlatMapOperator<in TIn, TOut> { void FlatMap(TIn value, ICollector<TOut> collector); }
    public interface IOperatorLifecycle { void Open(IRuntimeContext context); void Close(); }
    public interface ICheckpointableOperator
    {
        Task<string> SnapshotState(long checkpointId, IStateSnapshotWriter writer);
        Task RestoreState(long checkpointId, IStateSnapshotReader reader);
    }
}
namespace FlinkDotNet.Core.Abstractions.Sinks
{
    public interface ISinkContext { long CurrentProcessingTimeMillis(); }
    public interface ISinkFunction<in T> { void Invoke(T value, ISinkContext context); }
}
namespace FlinkDotNet.Core.Abstractions.Sources
{
    public interface ISourceContext<TOutput> { void Collect(TOutput record); void CollectBarrier(CheckpointBarrier barrier); }
    public interface ISourceFunction<TOutput> { void Run(ISourceContext<TOutput> ctx); void Cancel(); }
}
namespace FlinkDotNet.Core.Abstractions.Collectors
{
    public interface ICollector<in T>
    {
        Task Collect(T record, CancellationToken cancellationToken);
        Task CollectBarrier(CheckpointBarrier barrier, CancellationToken cancellationToken);
        Task CloseAsync();
    }
}

namespace FlinkDotNet.Core.Abstractions.Storage
{
    public interface IStateSnapshotWriter : IAsyncDisposable { Task BeginKeyedState(string stateName); Task WriteKeyedEntry(byte[] key, byte[] value); Task EndKeyedState(string stateName); Stream GetStateOutputStream(string stateName); Task<string> CommitAndGetHandleAsync(); }
    public interface IStateSnapshotReader : IDisposable { Task<bool> HasKeyedState(string stateName); IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ReadKeyedStateEntries(string stateName); Stream GetStateInputStream(string stateName); bool HasState(string stateName); }
    public interface IStateSnapshotStore { Task<IStateSnapshotWriter> CreateWriter(string jobId, long checkpointId, string operatorId, string subtaskSpecificId); Task<IStateSnapshotReader?> GetReader(string handle); }
    public class DummyStateSnapshotStore : IStateSnapshotStore { /* ... */
        public Task<IStateSnapshotWriter> CreateWriter(string jobId, long checkpointId, string operatorId, string subtaskSpecificId) => Task.FromResult<IStateSnapshotWriter>(new DummyStateSnapshotWriter());
        public Task<IStateSnapshotReader?> GetReader(string handle) => Task.FromResult<IStateSnapshotReader?>(new DummyStateSnapshotReader(handle, true));
    }
    public class DummyStateSnapshotWriter : IStateSnapshotWriter { /* ... */
        public Task BeginKeyedState(string stateName) => Task.CompletedTask;
        public Task WriteKeyedEntry(byte[] key, byte[] value) => Task.CompletedTask;
        public Task EndKeyedState(string stateName) => Task.CompletedTask;
        public Stream GetStateOutputStream(string stateName) => new MemoryStream();
        public Task<string> CommitAndGetHandleAsync() =>
            Task.FromResult("dummy_handle_" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture));
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
     public class DummyStateSnapshotReader : IStateSnapshotReader { /* ... */
        private readonly string _handle; private readonly bool _isEmpty;
        public DummyStateSnapshotReader(string handle, bool isEmpty = false) { _handle = handle; _isEmpty = isEmpty; }
        public Task<bool> HasKeyedState(string stateName) => Task.FromResult(!_isEmpty && false);
        public async IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ReadKeyedStateEntries(string stateName) { if (_isEmpty) yield break; await Task.CompletedTask; }
        public Stream GetStateInputStream(string stateName) { if (_isEmpty) throw new FileNotFoundException(); return new MemoryStream(); }
        public bool HasState(string stateName) => !_isEmpty && false;
        public void Dispose() { }
    }
}


namespace FlinkDotNet.TaskManager
{
    public interface ISerializerProvider { ITypeSerializer<T>? GetSerializer<T>(); ITypeSerializer? GetSerializer(Type t); }
    public class BasicSerializerProvider : ISerializerProvider { /* ... */
        private readonly Dictionary<Type, object> _serializers = new();
        public BasicSerializerProvider() { RegisterSerializer(new IntSerializer()); RegisterSerializer(new StringSerializer()); RegisterSerializer(new JsonPocoSerializer<object>()); }
        public void RegisterSerializer<T>(ITypeSerializer<T> serializer) => _serializers[typeof(T)] = serializer;
        public ITypeSerializer<T>? GetSerializer<T>() { _serializers.TryGetValue(typeof(T), out var s); return s != null ? (ITypeSerializer<T>)s : new JsonPocoSerializer<T>(); }
        public ITypeSerializer? GetSerializer(Type t) { _serializers.TryGetValue(t, out var s); if (s != null) return (ITypeSerializer)s; try { return (ITypeSerializer?)Activator.CreateInstance(typeof(JsonPocoSerializer<>).MakeGenericType(t)); } catch { return null; } }
    }

    public class NetworkedCollector<T> : ICollector<T> { /* ... */
        private readonly string _sourceJobVertexId; private readonly int _sourceSubtaskIndex; private readonly OperatorOutput _outputInfo; private readonly ITypeSerializer<T> _serializer; private readonly string _targetTmEndpoint;
        private DataExchangeService.DataExchangeServiceClient? _client; private AsyncClientStreamingCall<DataRecord, DataAck>? _streamCall;
        private readonly OutputEdgeKeyingInfo? _keyingInfo; private readonly Func<object, object?>? _keySelectorFunc; private readonly INetworkBufferPool _bufferPool;
        public NetworkedCollector( string sJVId, int sSubIdx, OperatorOutput oInfo, ITypeSerializer<T> ser, OutputEdgeKeyingInfo? ki, Func<object, object?>? ksf, INetworkBufferPool bp ) {
            _sourceJobVertexId = sJVId; _sourceSubtaskIndex = sSubIdx; _outputInfo = oInfo; _serializer = ser;  _targetTmEndpoint = oInfo.TargetTaskEndpoint; _keyingInfo = ki; _keySelectorFunc = ksf; _bufferPool = bp ?? throw new ArgumentNullException(nameof(bp));
        }
        private async Task EnsureOpen(CancellationToken ct) { if (_streamCall!=null) return; if(string.IsNullOrEmpty(_targetTmEndpoint)|| _targetTmEndpoint.Contains(":0")) return; try {var ch=GrpcChannel.ForAddress(_targetTmEndpoint);_client=new DataExchangeService.DataExchangeServiceClient(ch);_streamCall=_client.SendData(cancellationToken:ct);}catch{_streamCall=null;}}
        public async Task Collect(T rec, CancellationToken ct) {  await EnsureOpen(ct); if(_streamCall==null)return; int tgtIdx=_outputInfo.TargetSpecificSubtaskIndex; if(_keyingInfo!=null && _keySelectorFunc != null) { try{ object? k = _keySelectorFunc((object?)rec); tgtIdx=(k?.GetHashCode()??0&0x7FFFFFFF)%_keyingInfo.DownstreamParallelism; } catch (Exception ex){ Console.WriteLine($"ERROR keying: {ex.Message}"); tgtIdx=_outputInfo.TargetSpecificSubtaskIndex;} } try{await _streamCall.RequestStream.WriteAsync(new DataRecord{TargetJobVertexId=_outputInfo.TargetVertexId,TargetSubtaskIndex=tgtIdx,DataPayload=ByteString.CopyFrom(_serializer.Serialize(rec)),SourceJobVertexId=_sourceJobVertexId,SourceSubtaskIndex=_sourceSubtaskIndex});} catch{await CloseAsync();}}
        public async Task CollectBarrier(CheckpointBarrier b, CancellationToken ct) { await EnsureOpen(ct); if(_streamCall==null)return;try{await _streamCall.RequestStream.WriteAsync(new DataRecord{TargetJobVertexId=_outputInfo.TargetVertexId,TargetSubtaskIndex=_outputInfo.TargetSpecificSubtaskIndex,BarrierPayload=b,SourceJobVertexId=_sourceJobVertexId,SourceSubtaskIndex=_sourceSubtaskIndex});}catch{await CloseAsync();throw;}}
        public async Task CloseAsync() {if(_streamCall!=null){try{await _streamCall.RequestStream.CompleteAsync();await _streamCall.ResponseAsync;}catch{}finally{_streamCall.Dispose();_streamCall=null;}}}
    }

    public interface IRecordProcessor { Task ProcessDataRecordAsync(DataRecord dataRecord, CancellationToken cancellationToken); }
    public class SimpleSinkContext : ISinkContext { public long CurrentProcessingTimeMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(); }
    public class SimpleSourceContext<TOutput> : ISourceContext<TOutput> { /* ... */
        private readonly TaskExecutor _owner; private readonly CancellationToken _ct; private readonly IRuntimeContext _rtCtx;
        public SimpleSourceContext(TaskExecutor owner, CancellationToken ct, IRuntimeContext rtCtx) { _owner=owner; _ct=ct; _rtCtx=rtCtx; }
        public void Collect(TOutput rec) { _ = _owner.ProcessRecordFromSourceAsync(rec, _ct); }
        public void CollectBarrier(CheckpointBarrier b) { Console.WriteLine($"[{_rtCtx.TaskName}] SimpleSourceContext.CollectBarrier CP {b.CheckpointId} - unusual path."); }
    }
    public class CheckpointBarrierAligner { /* ... */
        public long CheckpointId { get; } private readonly HashSet<string> _expectedChannels; private readonly HashSet<string> _receivedChannels = new HashSet<string>();
        private readonly Func<Task> _onAlignedAction; private bool _actionTriggered = false; private readonly object _lock = new object();
        public CheckpointBarrierAligner(long checkpointId, IEnumerable<string> expectedChannelIds, Func<Task> onAlignedAction) { CheckpointId = checkpointId; _expectedChannels = new HashSet<string>(expectedChannelIds); _onAlignedAction = onAlignedAction; }
        public void RecordBarrierArrival(string channelId) { lock (_lock) { if (_actionTriggered || !_expectedChannels.Contains(channelId)) return; _receivedChannels.Add(channelId); if (_receivedChannels.SetEquals(_expectedChannels)) { _actionTriggered = true; Task.Run(_onAlignedAction); } } }
    }

    // MODIFIED OperatorChainInfo
    public class OperatorChainInfo
    {
        public object Instance { get; }
        public string OriginalJobVertexId { get; }
        public bool IsCheckpointable { get; }
        public string OperatorName { get; }
        public Type? InputType { get; } // Type expected by this operator
        public Type? OutputType { get; } // Type produced by this operator
        public ICollector<object>? FlatMapCollectorInstance { get; set; } // NEW FIELD

        public OperatorChainInfo(object instance, string originalJobVertexId, string operatorName, Type? inputType, Type? outputType)
        {
            Instance = instance;
            OriginalJobVertexId = originalJobVertexId;
            IsCheckpointable = instance is ICheckpointableOperator;
            OperatorName = operatorName;
            InputType = inputType;
            OutputType = outputType;
        }
    }

    public partial class TaskExecutor : IRecordProcessor
    {
        private readonly string _taskManagerId;
        private readonly ISerializerProvider _serializerProvider;
        private readonly KeySelectorActivator _keySelectorActivator = new KeySelectorActivator();

        private TaskDeploymentDescriptor? _tdd;
        private string _taskName = "UnnamedTask";
        private bool _isSource, _isOperator, _isSink;
        private IRuntimeContext? _runtimeContext;
        private ISinkContext? _sinkContext;
        private ITypeSerializer<object>? _inputDataSerializer;

        private List<OperatorChainInfo> _operatorChainWithMetadata = new List<OperatorChainInfo>();
        private object? _sourceInstance;
        private IMapOperator<object, object>? _operatorInstance;
        private ISinkFunction<object>? _sinkInstance;
        private List<ICollector<object>> _collectors = new List<ICollector<object>>();

        private readonly ConcurrentDictionary<long, CheckpointBarrierAligner> _barrierAligners = new ConcurrentDictionary<long, CheckpointBarrierAligner>();
        private List<string> _expectedInputChannelIds = new List<string>();
        private IStateSnapshotStore _stateStore;
        private INetworkBufferPool _networkBufferPool;
        private CancellationTokenSource _taskCts = new CancellationTokenSource(); // For managing task lifecycle including collectors

        public TaskExecutor(string taskManagerId, ISerializerProvider serializerProvider, IStateSnapshotStore? stateStore = null, INetworkBufferPool? networkBufferPool = null) {
            _taskManagerId = taskManagerId; _serializerProvider = serializerProvider ?? new BasicSerializerProvider();
            _stateStore = stateStore ?? new DummyStateSnapshotStore(); _networkBufferPool = networkBufferPool ?? new TaskManagerBufferPool();
        }

        public async Task<DeployTaskResponse> ExecuteFromDescriptor(TaskDeploymentDescriptor tdd, CancellationToken cancellationToken) {
            _tdd = tdd; _taskName = $"{_tdd.TaskName}_{_tdd.SubtaskIndex}";
            _taskCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken); // Link to overall task cancellation

            _isSource = !_tdd.Inputs.Any() && _tdd.Outputs.Any(); _isSink = _tdd.Inputs.Any() && !_tdd.Outputs.Any(); _isOperator = _tdd.Inputs.Any() && _tdd.Outputs.Any();
            _expectedInputChannelIds = _tdd.Inputs.Select(i => $"{i.UpstreamJobVertexId}_{i.UpstreamSubtaskIndex}").ToList();
            _runtimeContext = new BasicRuntimeContext(_tdd.JobGraphJobId, _taskName, 1, _tdd.SubtaskIndex);
            _operatorChainWithMetadata.Clear(); _collectors.Clear(); _inputKeySelectorDelegate = null;

            try {
                Type? headCompType = Type.GetType(tdd.FullyQualifiedOperatorName); if (headCompType == null) throw new Exception($"Head op type '{tdd.FullyQualifiedOperatorName}' not found.");
                object headOp = Activator.CreateInstance(headCompType)!;
                if (headOp is IOperatorLifecycle headLc) headLc.Open(_runtimeContext);

                Type? headInputType = string.IsNullOrEmpty(tdd.InputTypeName) ? null : Type.GetType(tdd.InputTypeName);
                Type? headOutputType = string.IsNullOrEmpty(tdd.OutputTypeName) && !tdd.ChainedOperatorInfo.Any() ? null : Type.GetType(tdd.OutputTypeName);
                // If chain exists, tdd.OutputTypeName is final. Head's output type needs to be known for first chained op.
                if (tdd.ChainedOperatorInfo.Any()) {
                     // This is complex. Assume first chained op's input type matches head's output for now.
                     // Or better, if headOp is IMapOperator<TIn, TOut>, reflect TOut.
                     // For now, if chain exists, head's output type is taken from first chained op's input type.
                     headOutputType = Type.GetType(tdd.ChainedOperatorInfo.First().InputTypeName);
                }

                _operatorChainWithMetadata.Add(new OperatorChainInfo(headOp, tdd.JobVertexId, headCompType.Name, headInputType, headOutputType));
                if (_isSource) _sourceInstance = headOp; else if (_isOperator) _operatorInstance = headOp as IMapOperator<object,object>; else _sinkInstance = headOp as ISinkFunction<object>;

                Type? prevOutType = headOutputType;

                for(int i=0; i < tdd.ChainedOperatorInfo.Count; i++){
                    var chainedProto = tdd.ChainedOperatorInfo[i];
                    Type? chainedType = Type.GetType(chainedProto.FullyQualifiedOperatorName); if(chainedType==null) throw new Exception($"Chained op type '{chainedProto.FullyQualifiedOperatorName}' not found.");
                    Type? currentOpInputType = Type.GetType(chainedProto.InputTypeName);
                    Type? currentOpOutputType = string.IsNullOrEmpty(chainedProto.OutputTypeName) ? null : Type.GetType(chainedProto.OutputTypeName);

                    if(currentOpInputType != null && prevOutType != null && !currentOpInputType.IsAssignableFrom(prevOutType)) Console.WriteLine($"WARN: Type mismatch for {chainedProto.FullyQualifiedOperatorName}. Expected {currentOpInputType.Name}, prev output {prevOutType.Name}");
                    object chainedOp = Activator.CreateInstance(chainedType)!;
                    if(chainedOp is IOperatorLifecycle clc) clc.Open(_runtimeContext);
                    _operatorChainWithMetadata.Add(new OperatorChainInfo(chainedOp, chainedProto.OriginalJobVertexId, chainedType.Name, currentOpInputType, currentOpOutputType));
                    prevOutType = currentOpOutputType;
                }

                // Setup FlatMapCollectors after the full chain is known
                for(int i=0; i < _operatorChainWithMetadata.Count; i++) {
                    var opInfo = _operatorChainWithMetadata[i];
                    if (opInfo.Instance is IFlatMapOperator<object, object>) // Assuming object, object for simplicity
                    {
                        if (i == _operatorChainWithMetadata.Count - 1) // Last in chain
                        {
                            opInfo.FlatMapCollectorInstance = new BroadcastingCollector(this._collectors, _taskCts.Token);
                        }
                        else
                        {
                            // Not strictly needing nextOpInfo.Instance for ChainedCollector constructor anymore
                            opInfo.FlatMapCollectorInstance = new ChainedCollector<object>(this, i + 1, _taskCts.Token);
                        }
                         Console.WriteLine($"[{_taskName}] Configured FlatMapCollector for {opInfo.OperatorName}");
                    }
                }


                if(!string.IsNullOrEmpty(tdd.InputTypeName)){ Type? inType = Type.GetType(tdd.InputTypeName); if(inType!=null) _inputDataSerializer = (ITypeSerializer<object>?)_serializerProvider.GetSerializer(inType); }
                ITypeSerializer<object>? finalOutSer = null;
                if(tdd.Outputs.Any()){
                    Type? finalOutTypeToSerialize = _operatorChainWithMetadata.LastOrDefault()?.OutputType ?? Type.GetType(tdd.OutputTypeName);
                    if(finalOutTypeToSerialize != null) finalOutSer = (ITypeSerializer<object>?)_serializerProvider.GetSerializer(finalOutTypeToSerialize);
                    if(finalOutSer==null) finalOutSer = new JsonPocoSerializer<object>();
                }
                if(_isSink || _operatorChainWithMetadata.Any(opInfo => opInfo.Instance is ISinkFunction<object>)) _sinkContext = new SimpleSinkContext();

                foreach(var outputDesc in tdd.Outputs){
                    Func<object,object?>? outputKeyer = null;
                    var keyInfo = outputDesc.OutputKeyingInfoCase == OperatorOutput.OutputKeyingInfoOneofCase.KeyingInfo ? outputDesc.KeyingInfo : null;
                    if(keyInfo!=null && !string.IsNullOrEmpty(keyInfo.SerializedKeySelector)) {
                        // OutputTypeName for key selector should be the type of data *before* it goes to NetworkedCollector,
                        // which is the output of the last operator in the chain.
                        string typeForOutputKeySelector = _operatorChainWithMetadata.LastOrDefault()?.OutputType?.AssemblyQualifiedName ?? tdd.OutputTypeName!;
                        outputKeyer = _keySelectorActivator.GetOrCreateKeySelector(keyInfo.SerializedKeySelector, typeForOutputKeySelector, keyInfo.KeyTypeName, _taskName);
                    }
                    if(finalOutSer != null) _collectors.Add(new NetworkedCollector<object>(tdd.JobVertexId, tdd.SubtaskIndex, outputDesc, finalOutSer, keyInfo, outputKeyer, _networkBufferPool));
                }

                if (tdd.InputKeyingInfo.Any() && !string.IsNullOrEmpty(tdd.InputKeyingInfo.First().SerializedKeySelector)) {
                   var primaryInputKeyingInfo = tdd.InputKeyingInfo.First();
                   _inputKeySelectorDelegate = _keySelectorActivator.GetOrCreateKeySelector(
                       primaryInputKeyingInfo.SerializedKeySelector, tdd.InputTypeName!, primaryInputKeyingInfo.KeyTypeName, _taskName);
                }

                if(_isSource && _sourceInstance != null){ var srcCtx = new SimpleSourceContext<object>(this, _taskCts.Token, _runtimeContext!); Task.Run(()=>((ISourceFunction<object>)_sourceInstance).Run(srcCtx), _taskCts.Token); }
                return new DeployTaskResponse{Success=true, Message="Task setup finished."};
            } catch (Exception ex) { Console.WriteLine($"Error setting up task {_taskName}: {ex.Message}"); return new DeployTaskResponse{Success=false, Message=ex.Message};}
        }

        public async Task ProcessDataRecordAsync(DataRecord dataRecord, CancellationToken cancellationToken) { /* ... */
            if (_tdd == null || _runtimeContext == null || _inputDataSerializer == null) { Console.WriteLine($"[{_taskName}] Task not initialized. Ignoring record."); return; }
            switch (dataRecord.PayloadCase) {
                case DataRecord.PayloadOneofCase.DataPayload:
                    NetworkBuffer? netBuf = null; object? deserRec = null; bool deserOk = false;
                    if (_networkBufferPool == null) { deserRec = _inputDataSerializer.Deserialize(dataRecord.DataPayload.ToByteArray()); deserOk = true; }
                    else {
                        netBuf = _networkBufferPool.RequestBuffer(dataRecord.DataPayload.Length);
                        if (netBuf != null) {
                            try {
                                netBuf.Reset(); dataRecord.DataPayload.CopyTo(netBuf.UnderlyingBuffer, netBuf.DataOffset); netBuf.SetDataLength(dataRecord.DataPayload.Length);
                                deserRec = _inputDataSerializer.Deserialize(netBuf.GetMemory().ToArray()); deserOk = true;
                            } catch (Exception ex) { Console.WriteLine($"ERROR deserializing: {ex.Message}"); }
                            finally { netBuf.Dispose(); }
                        } else { Console.WriteLine($"WARN: BufferPool exhausted. Dropping record."); }
                    }
                    if (deserOk && deserRec != null) {
                        bool keySet = false;
                        if (_inputKeySelectorDelegate != null) { try { object? key = _inputKeySelectorDelegate(deserRec); _runtimeContext.SetCurrentKey(key); keySet = true; } catch (Exception ex) { Console.WriteLine($"ERROR extracting input key: {ex.Message}"); } }
                        try { await ProcessRecordThroughChain(deserRec, cancellationToken); }
                        finally { if (keySet) _runtimeContext.SetCurrentKey(null); }
                    } else if (!deserOk) { Console.WriteLine($"Record processing skipped: deserialization failure or no buffer."); }
                    break;
                case DataRecord.PayloadOneofCase.BarrierPayload: await HandleIncomingBarrier(dataRecord, cancellationToken); break;
                default: Console.WriteLine($"Unknown payload: {dataRecord.PayloadCase}."); break;
            }
        }

        public async Task ProcessRecordFromSourceAsync(object? record, CancellationToken cancellationToken) {
            await ProcessRecordThroughChain(record, cancellationToken);
        }

        internal async Task ProcessRecordFromChainedCollector(object? record, int nextOperatorIndex, CancellationToken cancellationToken) { /* ... as before ... */
            await ProcessRecordThroughChain(record, cancellationToken, nextOperatorIndex);
        }

        private async Task ProcessRecordThroughChain(object? currentRecord, CancellationToken cancellationToken, int startIndex = -1) { /* ... as before, but uses FlatMapCollectorInstance ... */
            if (_runtimeContext == null) { Console.WriteLine($"CRITICAL: RuntimeContext null in ProcessRecordThroughChain."); return; }
            bool recordDropped = false;
            int currentOperatorIdx = startIndex;
            if (currentOperatorIdx == -1) {
                 currentOperatorIdx = (ReferenceEquals(_operatorChainWithMetadata.FirstOrDefault()?.Instance, _sourceInstance) && _sourceInstance != null) ? 1 : 0;
            }

            for (int i = currentOperatorIdx; i < _operatorChainWithMetadata.Count; i++) {
                var opInfo = _operatorChainWithMetadata[i];
                object op = opInfo.Instance;
                if (currentRecord == null && !(op is ISinkFunction<object>)) { /* Pass nulls */ }

                if (op is IMapOperator<object, object> mapOp) currentRecord = mapOp.Map(currentRecord);
                else if (op is IFilterOperator<object> filterOp) {
                    if (currentRecord == null || !filterOp.Filter(currentRecord)) { recordDropped = true; break; }
                }
                else if (op is IFlatMapOperator<object, object> flatMapOp)
                {
                    ICollector<object>? collectorToUse = opInfo.FlatMapCollectorInstance;

                    if (collectorToUse == null)
                    {
                        Console.WriteLine($"[{_taskName}] CRITICAL ERROR: FlatMapOperator {opInfo.OperatorName} (OriginalVID: {opInfo.OriginalJobVertexId}) has no configured FlatMapCollectorInstance at index {i}.");
                        if (i == _operatorChainWithMetadata.Count - 1 && _collectors.Any()) { // Last operator in chain
                            Console.WriteLine($"[{_taskName}] Attempting to use BroadcastingCollector for last-in-chain FlatMapOperator {opInfo.OperatorName} due to missing preconfigured collector.");
                            collectorToUse = new BroadcastingCollector(this._collectors, cancellationToken); // Use current processing CToken
                        } else { // Not last, or no external collectors. Cannot proceed for this FlatMap.
                            recordDropped = true;
                            break;
                        }
                    }

                    if (currentRecord != null) // FlatMap might handle null, but often expects non-null
                    {
                        flatMapOp.FlatMap(currentRecord, collectorToUse);
                    }
                    else // currentRecord is null
                    {
                        // Optionally, decide if FlatMap should be called with null or if it implies no operation.
                        // Flink's FlatMap typically processes non-null records.
                        // If FlatMap needs to be aware of nulls (e.g. for windowing side effects), it should handle it.
                        // For now, if record is null, we can assume FlatMap is not called, and since it's a FlatMap,
                        // no specific output is generated from this null input, effectively 'dropping' the null for this path.
                    }
                    recordDropped = true;
                    break;
                }
                else if (op is ISinkFunction<object> sinkFunc) {
                    sinkFunc.Invoke(currentRecord, _sinkContext ?? new SimpleSinkContext()); recordDropped = true; break;
                } else if (i < _operatorChainWithMetadata.Count - 1) {
                    Console.WriteLine($"WARNING: Unknown operator type {op.GetType().Name} in chain. Passing record.");
                }
            }
            if (!recordDropped && _collectors.Any()) {
                foreach (var collector in _collectors) await collector.Collect(currentRecord!, cancellationToken);
            }
        }

        private async Task HandleIncomingBarrier(DataRecord barrierRecord, CancellationToken cancellationToken) { /* ... */
            if (_tdd == null) return; CheckpointBarrier barrier = barrierRecord.BarrierPayload;
            string inputChannelId = $"{barrierRecord.SourceJobVertexId}_{barrierRecord.SourceSubtaskIndex}";
            var aligner = _barrierAligners.GetOrAdd(barrier.CheckpointId, cpId => new CheckpointBarrierAligner(cpId, _expectedInputChannelIds, async () => { await SnapshotChainStateAndForwardBarrierAsync(barrier, cancellationToken); _barrierAligners.TryRemove(cpId, out _); }));
            aligner.RecordBarrierArrival(inputChannelId);
        }

        private async Task SnapshotChainStateAndForwardBarrierAsync(CheckpointBarrier barrier, CancellationToken cancellationToken) { /* ... */
            if (_tdd == null || _runtimeContext == null) { Console.WriteLine($"CRITICAL: TDD/RuntimeContext null. Cannot snapshot for CP {barrier.CheckpointId}."); return; }
            bool allOk = true;
            foreach (var opInfo in _operatorChainWithMetadata) {
                if (opInfo.IsCheckpointable) {
                    var checkpointableOp = (ICheckpointableOperator)opInfo.Instance;
                    try {
                        string subtaskSpecificId = $"{_tdd.JobVertexId}_{_tdd.SubtaskIndex}";
                        await using (IStateSnapshotWriter writer = await _stateStore.CreateWriter(_tdd.JobGraphJobId, barrier.CheckpointId, opInfo.OriginalJobVertexId, subtaskSpecificId)) {
                            string handle = await checkpointableOp.SnapshotState(barrier.CheckpointId, writer);
                            Console.WriteLine($"SIMULATE ACK: CP {barrier.CheckpointId} for {opInfo.OriginalJobVertexId}. Handle: {handle}");
                        }
                    } catch (Exception ex) { Console.WriteLine($"ERROR snapshotting {opInfo.OperatorName} (ID: {opInfo.OriginalJobVertexId}): {ex.Message}"); allOk = false; break; }
                }
            }
            if (!allOk) { Console.WriteLine($"CP {barrier.CheckpointId}: Chain snapshot failed. Barrier not forwarded."); return; }
            if (_collectors.Any()) {
                var fTasks = _collectors.Select(c => c.CollectBarrier(barrier, cancellationToken)).ToList();
                try { await Task.WhenAll(fTasks); Console.WriteLine($"CP {barrier.CheckpointId}: Barrier forwarded externally."); }
                catch (Exception ex) { Console.WriteLine($"ERROR forwarding barrier {barrier.CheckpointId} externally: {ex.Message}"); return; }
            } else Console.WriteLine($"CP {barrier.CheckpointId}: Chain has no external outputs.");
            if (allOk && !_isSource && _operatorChainWithMetadata.All(op => !op.IsCheckpointable) && !_operatorChainWithMetadata.Any(op=> op.Instance is ISinkFunction<object>)) {
                 Console.WriteLine($"CP {barrier.CheckpointId}: Stateless non-source/non-sink chain processed barrier. Head task might ACK.");
            }
        }

        // Nested ChainedCollector class
        private class ChainedCollector<TCollectedOut> : ICollector<TCollectedOut>
        {
            private readonly TaskExecutor _taskExecutorInstance;
            private readonly int _nextOperatorIndexInChain;
            private readonly CancellationToken _cancellationToken;

            public ChainedCollector(
                TaskExecutor taskExecutorInstance,
                int nextOperatorIndexInChain,
                CancellationToken cancellationToken)
            {
                _taskExecutorInstance = taskExecutorInstance ?? throw new ArgumentNullException(nameof(taskExecutorInstance));
                if (nextOperatorIndexInChain < 0) throw new ArgumentOutOfRangeException(nameof(nextOperatorIndexInChain));
                _nextOperatorIndexInChain = nextOperatorIndexInChain;
                _cancellationToken = cancellationToken;
            }

            public Task Collect(TCollectedOut record, CancellationToken cancellationToken)
            {
                if (_cancellationToken.IsCancellationRequested || cancellationToken.IsCancellationRequested)
                {
                    return Task.CompletedTask;
                }
                return _taskExecutorInstance.ProcessRecordFromChainedCollector(record, _nextOperatorIndexInChain, _cancellationToken);
            }

            public Task CollectBarrier(CheckpointBarrier barrier, CancellationToken cancellationToken)
            {
                Console.WriteLine($"[TaskExecutor.ChainedCollector] WARNING: CollectBarrier called. Barriers are managed by TaskExecutor. Ignored for CP {barrier.CheckpointId}.");
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                return Task.CompletedTask;
            }
        }

        // Nested BroadcastingCollector class
        private class BroadcastingCollector : ICollector<object>
        {
            private readonly List<ICollector<object>> _targetCollectors;
            private readonly CancellationToken _cancellationToken; // Task-level cancellation

            public BroadcastingCollector(List<ICollector<object>> targetCollectors, CancellationToken cancellationToken)
            {
                _targetCollectors = targetCollectors ?? throw new ArgumentNullException(nameof(targetCollectors));
                _cancellationToken = cancellationToken;
            }

            public async Task Collect(object record, CancellationToken cancellationToken) // Parameter cancellationToken is per-call
            {
                // Use linked token if both should be respected, or prioritize one.
                // For simplicity, using the per-call token if provided and valid.
                var effectiveToken = cancellationToken != CancellationToken.None ? cancellationToken : _cancellationToken;

                if (effectiveToken.IsCancellationRequested) return;

                var tasks = new List<Task>();
                foreach (var collector in _targetCollectors)
                {
                    // Pass the effective token to downstream collectors
                    tasks.Add(collector.Collect(record, effectiveToken));
                }
                await Task.WhenAll(tasks);
            }

            public async Task CollectBarrier(CheckpointBarrier barrier, CancellationToken cancellationToken) {
                var effectiveToken = cancellationToken != CancellationToken.None ? cancellationToken : _cancellationToken;
                if (effectiveToken.IsCancellationRequested) return;
                var tasks = new List<Task>();
                foreach (var collector in _targetCollectors) {
                    tasks.Add(collector.CollectBarrier(barrier, effectiveToken));
                }
                await Task.WhenAll(tasks);
            }

            public async Task CloseAsync() {
                if (_cancellationToken.IsCancellationRequested) return;
                var tasks = new List<Task>();
                foreach (var collector in _targetCollectors) {
                    tasks.Add(collector.CloseAsync());
                }
                await Task.WhenAll(tasks);
            }
        }

    } // End of TaskExecutor
}

// Dummy interfaces if not defined elsewhere, for Abstractions
namespace FlinkDotNet.Core.Abstractions.Operators { /* ... IMapOperator, IOperatorLifecycle, ICheckpointableOperator ... */ }
namespace FlinkDotNet.Core.Abstractions.Sinks { /* ... ISinkFunction, ISinkContext ... */ }
namespace FlinkDotNet.Core.Abstractions.Sources { /* ... ISourceFunction, ISourceContext ... */ }
// ICollector is now locally defined for TaskExecutor's scope, or assumed from Abstractions.Collectors if it matches.

#nullable disable
