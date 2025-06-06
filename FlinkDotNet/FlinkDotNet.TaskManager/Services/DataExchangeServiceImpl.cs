#nullable enable
using System;
using System.Threading.Tasks;
using Grpc.Core;
using FlinkDotNet.Proto.Internal;
using System.Collections.Concurrent; // For routing table/queues
using System.Diagnostics.Metrics; // Added for Metrics
using System.Text; // For Encoding

namespace FlinkDotNet.TaskManager.Services
{
    internal static class DataExchangeMetrics
    {
        private static readonly Meter DataExchangeMeter = new Meter("FlinkDotNet.TaskManager.DataExchangeService", "1.0.0");
        internal static readonly Counter<long> RecordsReceived = DataExchangeMeter.CreateCounter<long>("flinkdotnet.taskmanager.records_received", unit: "{records}", description: "Number of records received by DataExchangeServiceImpl.");
    }

    // This delegate will be invoked when a record is received for a specific task.
    // It should point to a method in TaskExecutor or a specific running task instance.
    public delegate Task ProcessRecordDelegate(string targetJobVertexId, int targetSubtaskIndex, byte[] payload);

    public static class DataReceiverRegistry
    {
        // Key: $"{jobVertexId}_{subtaskIndex}"
        // Value: The delegate that knows how to process the record for this specific task instance.
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

using FlinkDotNet.Core.Networking; // For LocalBufferPool, NetworkBufferPool, NetworkBuffer

namespace FlinkDotNet.TaskManager.Services
{
    public class DataExchangeServiceImpl : DataExchangeService.DataExchangeServiceBase
    {
        private readonly string _taskManagerId;
        private readonly NetworkBufferPool _globalNetworkBufferPool; // Added for LocalBufferPool creation

        public DataExchangeServiceImpl(string taskManagerId, NetworkBufferPool globalNetworkBufferPool)
        {
            _taskManagerId = taskManagerId ?? throw new ArgumentNullException(nameof(taskManagerId));
            _globalNetworkBufferPool = globalNetworkBufferPool ?? throw new ArgumentNullException(nameof(globalNetworkBufferPool));
            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Initialized.");
        }

        // Old SendData method removed / can be marked Obsolete if needed for a transition period.

        public override async Task ExchangeData(
            IAsyncStreamReader<UpstreamPayload> requestStream,
            IServerStreamWriter<DownstreamPayload> responseStream,
            ServerCallContext context)
        {
            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] New ExchangeData stream started by {context.Peer}.");

            // TODO: Determine targetJobVertexId and targetSubtaskIndex for this specific channel.
            // This might require an initial message from client, or be part of the gRPC path/metadata.
            // For now, we'll extract it from the first DataRecord, which is not ideal.
            string? targetJobVertexId = null;
            int targetSubtaskIndex = -1;

            // TODO: Configurable pool sizes per channel. Using defaults for now.
            // These defaults should be small for testing credit logic.
            const int localPoolMinSegments = 2;
            const int localPoolMaxSegments = 4; // Small to force credit logic. Flink uses numExclusiveBuffersPerChannel + floatingBuffers.

            LocalBufferPool? localBufferPool = null;
            Action? bufferReturnedCallback = null; // To send credit update

            try
            {
                bufferReturnedCallback = () =>
                {
                    // This callback is invoked by LocalBufferPool when a buffer is returned to it.
                    // We need to send a credit update back to the client.
                    // This needs to be careful about concurrency with responseStream.
                    try
                    {
                        // Send 1 credit for each buffer returned.
                        // In a real system, might batch credits or have more complex logic.
                        responseStream.WriteAsync(new DownstreamPayload { Credits = new CreditUpdate { CreditsGranted = 1 } })
                            .GetAwaiter().GetResult(); // Blocking for simplicity in callback, consider async queue for production
                        Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Sent 1 credit back to {context.Peer}.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error sending credit update: {ex.Message}");
                        // Cannot easily propagate this error back to the main loop from here without more complex plumbing.
                    }
                };

                // The LocalBufferPool constructor needs to be adapted if it's to use such a callback directly.
                // For now, this callback is conceptual. The LBP's ReturnSegmentToLocalPool
                // will be called, and *that* method (or the LBP itself) would need to invoke this.
                // This means LocalBufferPool needs a way to signal back.
                // Let's assume LocalBufferPool is modified to accept this Action in its constructor or a setter.
                // For this subtask, we'll focus on the DataExchangeService logic and assume LBP can trigger it.
                // A simpler way: DataExchangeService periodically checks localBufferPool.AvailablePoolBuffers and sends diff.
                // For now, we'll proceed with the direct callback assumption and refine LBP later if needed.

                await foreach (var upstreamPayload in requestStream.ReadAllAsync(context.CancellationToken))
                {
                    if (context.CancellationToken.IsCancellationRequested) break;

                    if (upstreamPayload.PayloadCase == UpstreamPayload.PayloadOneofCase.Record)
                    {
                        DataRecord dataRecord = upstreamPayload.Record;

                        if (localBufferPool == null) // First record, initialize pool and send initial credit
                        {
                            targetJobVertexId = dataRecord.TargetJobVertexId;
                            targetSubtaskIndex = dataRecord.TargetSubtaskIndex;
                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Channel mapped to {targetJobVertexId}_{targetSubtaskIndex}.");

                            // TODO: Pass bufferReturnedCallback to LocalBufferPool constructor or a method.
                            // This is a conceptual step for now.
                            // Action<LocalBufferPool, int> creditCallback = (pool, credits) => ... defined above or inline
                            localBufferPool = new LocalBufferPool(
                                _globalNetworkBufferPool,
                                localPoolMinSegments,
                                localPoolMaxSegments,
                                bufferReturnedCallback, // Pass the actual callback
                                $"LBP-Peer-{context.Peer}"); // Example pool identifier
                            // localBufferPool.SetBufferReturnedListener(bufferReturnedCallback); // Hypothetical method no longer needed if passed via ctor

                            int initialCredits = localBufferPool.AvailablePoolBuffers;
                            if (initialCredits > 0)
                            {
                                await responseStream.WriteAsync(new DownstreamPayload { Credits = new CreditUpdate { CreditsGranted = initialCredits } });
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Sent initial {initialCredits} credits to {context.Peer}.");
                            }
                        }

                        if (localBufferPool == null) {
                             Console.WriteLine($"[DataExchangeService-{_taskManagerId}] CRITICAL ERROR: localBufferPool is null even after first record. This should not happen. Peer: {context.Peer}");
                             throw new InvalidOperationException("LocalBufferPool was not initialized.");
                        }

                        NetworkBuffer networkBuffer;
                        try
                        {
                            // Use RequestBufferAsync to wait for a buffer if necessary
                            networkBuffer = await localBufferPool.RequestBufferAsync(context.CancellationToken);
                        }
                        catch (OperationCanceledException)
                        {
                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] RequestBufferAsync cancelled for {targetJobVertexId}_{targetSubtaskIndex}. Peer: {context.Peer}");
                            break; // Exit foreach loop as operation was cancelled
                        }
                        catch (ObjectDisposedException)
                        {
                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] LocalBufferPool disposed while requesting buffer for {targetJobVertexId}_{targetSubtaskIndex}. Peer: {context.Peer}");
                            break; // Exit foreach loop
                        }

                        try
                        {
                            // Copy data from DataRecord to NetworkBuffer
                            var payloadBytes = dataRecord.DataPayload.ToByteArray();
                            if (networkBuffer.Capacity < payloadBytes.Length)
                            {
                                // This should ideally not happen if segments are reasonably sized
                                // or if large messages are chunked by the sender.
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] ERROR: Record size ({payloadBytes.Length}) exceeds buffer capacity ({networkBuffer.Capacity}). Discarding record. Peer: {context.Peer}");
                                networkBuffer.Dispose(); // Return the buffer
                                continue;
                            }
                            networkBuffer.Write(payloadBytes);

                            // TODO: Handle dataRecord.IsCheckpointBarrier and dataRecord.BarrierPayload
                            // TODO: Handle dataRecord.Backlog

                            string receiverKey = $"{dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}";
                            if (DataReceiverRegistry.RecordProcessors.TryGetValue(receiverKey, out var processor))
                            {
                                DataExchangeMetrics.RecordsReceived.Add(1);

                                // Create a copy of the data for the processor, then dispose the network buffer.
                                // This ensures the buffer is returned to the LocalBufferPool promptly,
                                // which in turn allows the credit to be sent back to the sender.
                                byte[] payloadCopy = new byte[networkBuffer.DataLength];
                                networkBuffer.GetReadOnlyMemory().CopyTo(payloadCopy);

                                networkBuffer.Dispose(); // Dispose *before* calling processor, as data is copied.

                                await processor(dataRecord.TargetJobVertexId, dataRecord.TargetSubtaskIndex, payloadCopy);
                            }
                            else
                            {
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] No record processor registered for {receiverKey}. Discarding record. Peer: {context.Peer}");
                                networkBuffer.Dispose(); // Still need to dispose buffer
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error processing record for {targetJobVertexId}_{targetSubtaskIndex}: {ex.Message}. Peer: {context.Peer}");
                            if (!networkBuffer.IsDisposed) networkBuffer.Dispose(); // Ensure buffer is disposed on error
                        }
                    }
                    // TODO: Handle other UpstreamPayload types if any (e.g., control messages from client)
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] ExchangeData stream cancelled by client or server shutdown for {context.Peer}.");
            }
            catch (Exception ex) when (ex is not RpcException) // Catch other non-gRPC exceptions
            {
                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error in ExchangeData stream with {context.Peer}: {ex.Message} {ex.StackTrace}");
            }
            finally
            {
                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] ExchangeData stream from {context.Peer} ended.");
                localBufferPool?.Dispose(); // Clean up the local pool for this connection
            }
        }
    }
}
#nullable disable
