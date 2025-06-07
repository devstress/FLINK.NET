using System;
using System.Threading.Tasks;
using Grpc.Core;
using FlinkDotNet.Proto.Internal;
using System.Collections.Concurrent; // For routing table/queues
using System.Diagnostics.Metrics; // Added for Metrics
using System.Text; // For Encoding
using FlinkDotNet.Core.Networking; // For LocalBufferPool, NetworkBufferPool, NetworkBuffer

namespace FlinkDotNet.TaskManager.Services
{
    internal static class DataExchangeMetrics
    {
        private static readonly Meter DataExchangeMeter = new Meter("FlinkDotNet.TaskManager.DataExchangeService", "1.0.0");
        internal static readonly Counter<long> RecordsReceived = DataExchangeMeter.CreateCounter<long>("flinkdotnet.taskmanager.records_received", unit: "{records}", description: "Number of records received by DataExchangeServiceImpl.");
    }

    public delegate Task ProcessRecordDelegate(string targetJobVertexId, int targetSubtaskIndex, byte[] payload);

    public static class DataReceiverRegistry
    {
        public static ConcurrentDictionary<string, ProcessRecordDelegate> RecordProcessors { get; } = new();

        public static void RegisterReceiver(string jobVertexId, int subtaskIndex, ProcessRecordDelegate processor)
        {
            RecordProcessors[$"{jobVertexId}_{subtaskIndex}"] = processor;
            Console.WriteLine($"[DataReceiverRegistry] Registered record processor for {jobVertexId}_{subtaskIndex}");
        }

        public static void UnregisterReceiver(string jobVertexId, int subtaskIndex)
        {
            RecordProcessors.TryRemove($"{jobVertexId}_{subtaskIndex}", out _);
            Console.WriteLine($"[DataReceiverRegistry] Unregistered record processor for {jobVertexId}_{subtaskIndex}");
        }
    }

    public class DataExchangeServiceImpl : DataExchangeService.DataExchangeServiceBase
    {
        private readonly string _taskManagerId;
        private readonly NetworkBufferPool _globalNetworkBufferPool;
        private readonly TaskExecutor _taskExecutor; 

        public DataExchangeServiceImpl(string taskManagerId, NetworkBufferPool globalNetworkBufferPool, TaskExecutor taskExecutor)
        {
            _taskManagerId = taskManagerId ?? throw new ArgumentNullException(nameof(taskManagerId));
            _globalNetworkBufferPool = globalNetworkBufferPool ?? throw new ArgumentNullException(nameof(globalNetworkBufferPool));
            _taskExecutor = taskExecutor ?? throw new ArgumentNullException(nameof(taskExecutor)); 
            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Initialized.");
        }

        public override async Task ExchangeData(
            IAsyncStreamReader<UpstreamPayload> requestStream,
            IServerStreamWriter<DownstreamPayload> responseStream,
            ServerCallContext context)
        {
            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] New ExchangeData stream started by {context.Peer}.");

            string? targetJobVertexId = null;
            int targetSubtaskIndex = -1;

            const int localPoolMinSegments = 2;
            const int localPoolMaxSegments = 4; 

            LocalBufferPool? localBufferPool = null;
            Action? bufferReturnedCallback = null; 

            try
            {
                bufferReturnedCallback = () =>
                {
                    try
                    {
                        responseStream.WriteAsync(new DownstreamPayload { Credits = new CreditUpdate { CreditsGranted = 1 } })
                            .GetAwaiter().GetResult(); 
                        Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Sent 1 credit back to {context.Peer}.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error sending credit update: {ex.Message}");
                    }
                };

                await foreach (var upstreamPayload in requestStream.ReadAllAsync(context.CancellationToken))
                {
                    if (context.CancellationToken.IsCancellationRequested) break;

                    if (upstreamPayload.PayloadCase == UpstreamPayload.PayloadOneofCase.Record)
                    {
                        DataRecord dataRecord = upstreamPayload.Record;

                        if (dataRecord.IsCheckpointBarrier)
                        {
                            var barrierProto = dataRecord.BarrierPayload;
                            if (barrierProto == null)
                            {
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Received DataRecord for {dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex} marked as barrier but BarrierPayload is null. Discarding. Peer: {context.Peer}");
                                continue; 
                            }

                            if (targetJobVertexId == null) { 
                                targetJobVertexId = dataRecord.TargetJobVertexId;
                                targetSubtaskIndex = dataRecord.TargetSubtaskIndex;
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Channel mapped to {targetJobVertexId}_{targetSubtaskIndex} from first barrier. Peer: {context.Peer}");
                            }

                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Received BARRIER for {dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}. CP ID: {barrierProto.CheckpointId}, Timestamp: {barrierProto.CheckpointTimestamp}, From Input: {dataRecord.SourceJobVertexId}_{dataRecord.SourceSubtaskIndex}. Peer: {context.Peer}");

                            var handler = _taskExecutor.GetOperatorBarrierHandler(dataRecord.TargetJobVertexId, dataRecord.TargetSubtaskIndex);
                            if (handler != null)
                            {
                                string barrierSourceInputId = $"{dataRecord.SourceJobVertexId}_{dataRecord.SourceSubtaskIndex}";
                                _ = handler.HandleIncomingBarrier(barrierProto.CheckpointId, barrierProto.CheckpointTimestamp, barrierSourceInputId);
                            }
                            else
                            {
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] No OperatorBarrierHandler found for {dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex} to handle barrier {barrierProto.CheckpointId}. This might be a sink or a source if barriers are routed unexpectedly. Peer: {context.Peer}");
                            }
                            continue;
                        }
                        // Check for WatermarkPayload
                        else if (dataRecord.PayloadTypeCase == DataRecord.PayloadTypeOneofCase.WatermarkPayload)
                        {
                            if (targetJobVertexId == null) { // First message determines target
                                targetJobVertexId = dataRecord.TargetJobVertexId;
                                targetSubtaskIndex = dataRecord.TargetSubtaskIndex;
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Channel mapped to {targetJobVertexId}_{targetSubtaskIndex} from first watermark. Peer: {context.Peer}");
                            }

                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Received WATERMARK for {dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}. Timestamp: {dataRecord.WatermarkPayload.Timestamp}. Peer: {context.Peer}");
                            // TODO: Implement watermark handling. This involves:
                            // 1. Forwarding the watermark to the target operator instance (TaskExecutor).
                            // 2. The operator instance then needs to update its internal event time clock and potentially trigger windows or timers.
                            // 3. For now, it's just logged.
                            continue; // Watermarks are control messages, don't go through buffer pool/data processing path here
                        }
                        // Handle DataPayload
                        else
                        {
                            if (localBufferPool == null)
                            {
                                if (targetJobVertexId == null) {
                                    targetJobVertexId = dataRecord.TargetJobVertexId;
                                    targetSubtaskIndex = dataRecord.TargetSubtaskIndex;
                                }
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Channel mapped to {targetJobVertexId}_{targetSubtaskIndex}. Initializing LocalBufferPool for data. Peer: {context.Peer}");

                                localBufferPool = new LocalBufferPool(
                                    _globalNetworkBufferPool,
                                    localPoolMinSegments,
                                    localPoolMaxSegments,
                                    bufferReturnedCallback,
                                    $"LBP-Peer-{context.Peer}");

                                int initialCredits = localBufferPool.AvailablePoolBuffers;
                                if (initialCredits > 0)
                                {
                                    await responseStream.WriteAsync(new DownstreamPayload { Credits = new CreditUpdate { CreditsGranted = initialCredits } });
                                    Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Sent initial {initialCredits} credits to {context.Peer}.");
                                }
                            }

                            if (localBufferPool == null) {
                                 Console.WriteLine($"[DataExchangeService-{_taskManagerId}] CRITICAL ERROR: localBufferPool is null even after first data record. Peer: {context.Peer}");
                                 throw new InvalidOperationException("LocalBufferPool was not initialized for data payload.");
                            }

                            NetworkBuffer networkBuffer;
                            try
                            {
                                networkBuffer = await localBufferPool.RequestBufferAsync(context.CancellationToken);
                            }
                            catch (OperationCanceledException)
                            {
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] RequestBufferAsync cancelled for {targetJobVertexId}_{targetSubtaskIndex}. Peer: {context.Peer}");
                                break;
                            }
                            catch (ObjectDisposedException)
                            {
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] LocalBufferPool disposed while requesting buffer for {targetJobVertexId}_{targetSubtaskIndex}. Peer: {context.Peer}");
                                break;
                            }

                            try
                            {
                                var payloadBytes = dataRecord.DataPayload.ToByteArray();
                                if (networkBuffer.Capacity < payloadBytes.Length)
                                {
                                    Console.WriteLine($"[DataExchangeService-{_taskManagerId}] ERROR: Record size ({payloadBytes.Length}) exceeds buffer capacity ({networkBuffer.Capacity}). Discarding record. Peer: {context.Peer}");
                                    networkBuffer.Dispose();
                                    continue;
                                }
                                networkBuffer.Write(payloadBytes);

                                string receiverKey = $"{dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}";
                                if (DataReceiverRegistry.RecordProcessors.TryGetValue(receiverKey, out var processor))
                                {
                                    DataExchangeMetrics.RecordsReceived.Add(1);

                                    byte[] payloadCopy = new byte[networkBuffer.DataLength];
                                    networkBuffer.GetReadOnlyMemory().CopyTo(payloadCopy);

                                    networkBuffer.Dispose();

                                    await processor(dataRecord.TargetJobVertexId, dataRecord.TargetSubtaskIndex, payloadCopy);
                                }
                                else
                                {
                                    Console.WriteLine($"[DataExchangeService-{_taskManagerId}] No record processor registered for {receiverKey}. Discarding record. Peer: {context.Peer}");
                                    networkBuffer.Dispose();
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error processing record for {targetJobVertexId}_{targetSubtaskIndex}: {ex.Message}. Peer: {context.Peer}");
                                if (networkBuffer != null && !networkBuffer.IsDisposed) networkBuffer.Dispose();
                            }
                        }
                    }
                    // TODO: Handle other UpstreamPayload types if any (e.g., control messages from client)
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] ExchangeData stream cancelled by client or server shutdown for {context.Peer}.");
            }
            catch (Exception ex) when (ex is not RpcException) 
            {
                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error in ExchangeData stream with {context.Peer}: {ex.Message} {ex.StackTrace}");
            }
            finally
            {
                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] ExchangeData stream from {context.Peer} ended.");
                localBufferPool?.Dispose(); 
            }
        }
    }
}
#nullable disable
