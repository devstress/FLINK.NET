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

                    string receiverKey = $"{dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}";
                    if (DataReceiverRegistry.RecordProcessors.TryGetValue(receiverKey, out var processor))
                    {
                        try
                        {
                            if (dataRecord.IsCheckpointBarrier)
                            {
                                // It's a Protobuf barrier
                                var barrierInfo = dataRecord.BarrierPayload; // Assuming BarrierPayload is never null if IsCheckpointBarrier is true
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Received PROTOFBUF BARRIER for {receiverKey}. ID: {barrierInfo?.CheckpointId}, Timestamp: {barrierInfo?.CheckpointTimestamp}");

                                // For PoC: Forward a special string payload indicating a barrier from proto
                                // A more robust solution would change ProcessRecordDelegate signature or use a typed envelope.
                                string barrierMarkerPayload = $"PROTO_BARRIER_{barrierInfo?.CheckpointId}_{barrierInfo?.CheckpointTimestamp}";
                                byte[] payloadBytes = Encoding.UTF8.GetBytes(barrierMarkerPayload);

                                await processor(dataRecord.TargetJobVertexId, dataRecord.TargetSubtaskIndex, payloadBytes);
                                // Note: DataExchangeMetrics.RecordsReceived currently counts barriers if not filtered.
                                // The metric was moved to the 'else' block to only count actual data records.
                            }
                            else
                            {
                                // Regular data record
                                // Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Received data for {receiverKey}, Size: {dataRecord.Payload.Length}");
                                DataExchangeMetrics.RecordsReceived.Add(1); // Count only actual data records
                                await processor(dataRecord.TargetJobVertexId, dataRecord.TargetSubtaskIndex, dataRecord.Payload.ToByteArray());
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error processing record/barrier for {receiverKey}: {ex.Message}");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[DataExchangeService-{_taskManagerId}] No record processor registered for {receiverKey}. Discarding record/barrier.");
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
