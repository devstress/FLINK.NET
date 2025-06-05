#nullable enable
using System;
using System.Threading.Tasks;
using Grpc.Core;
using FlinkDotNet.Proto.Internal;
using System.Collections.Concurrent; // For routing table/queues

namespace FlinkDotNet.TaskManager.Services
{
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

    public class DataExchangeServiceImpl : DataExchangeService.DataExchangeServiceBase
    {
        private readonly string _taskManagerId;

        public DataExchangeServiceImpl(string taskManagerId)
        {
            _taskManagerId = taskManagerId;
            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Initialized.");
        }

        public override async Task<DataAck> SendData(
            IAsyncStreamReader<DataRecord> requestStream,
            ServerCallContext context)
        {
            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Incoming SendData stream started by {context.Peer}.");
            try
            {
                await foreach (var dataRecord in requestStream.ReadAllAsync(context.CancellationToken))
                {
                    if (context.CancellationToken.IsCancellationRequested) break;

                    // Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Received record for {dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}, Size: {dataRecord.Payload.Length}");

                    string receiverKey = $"{dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}";
                    if (DataReceiverRegistry.RecordProcessors.TryGetValue(receiverKey, out var processor))
                    {
                        try
                        {
                            await processor(dataRecord.TargetJobVertexId, dataRecord.TargetSubtaskIndex, dataRecord.Payload.ToByteArray());
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error processing record for {receiverKey}: {ex.Message}");
                            // Decide on error handling: ignore, propagate, stop stream?
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[DataExchangeService-{_taskManagerId}] No record processor registered for {receiverKey}. Discarding record.");
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException && ex is not RpcException)
            {
                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error reading client stream: {ex.Message}");
                // Potentially return an error status or throw RpcException
            }
            finally
            {
                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] SendData stream from {context.Peer} ended.");
            }
            return new DataAck { Received = true }; // Indicates stream processing finished on server side.
        }
    }
}
#nullable disable
