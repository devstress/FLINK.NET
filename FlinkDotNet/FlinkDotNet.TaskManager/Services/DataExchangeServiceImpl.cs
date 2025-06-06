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
        private readonly TaskExecutor _taskExecutor;

        public DataExchangeServiceImpl(string taskManagerId, TaskExecutor taskExecutor)
        {
            _taskManagerId = taskManagerId;
            _taskExecutor = taskExecutor;
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

                    // string receiverKey = $"{dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}"; // already exists

                    if (dataRecord.IsCheckpointBarrier)
                    {
                        var barrierProto = dataRecord.BarrierPayload;
                        if (barrierProto == null) {
                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Received DataRecord for {dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex} marked as barrier but BarrierPayload is null. Discarding.");
                            continue; // Skip this record
                        }

                        var receiverKeyForLog = $"{dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}"; // For logging consistency
                        Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Received BARRIER for {receiverKeyForLog}. CP ID: {barrierProto.CheckpointId}, Timestamp: {barrierProto.CheckpointTimestamp}, From Input: {dataRecord.SourceJobVertexId}_{dataRecord.SourceSubtaskIndex}");

                        var handler = _taskExecutor.GetOperatorBarrierHandler(dataRecord.TargetJobVertexId, dataRecord.TargetSubtaskIndex);
                        if (handler != null)
                        {
                            string barrierSourceInputId = $"{dataRecord.SourceJobVertexId}_{dataRecord.SourceSubtaskIndex}";
                            // Intentionally not awaiting. HandleIncomingBarrier itself can use Task.Run for long operations.
                            _ = handler.HandleIncomingBarrier(barrierProto.CheckpointId, barrierProto.CheckpointTimestamp, barrierSourceInputId);
                        }
                        else
                        {
                            // This might be normal if the target is a sink that doesn't have complex barrier alignment,
                            // or if the task is still initializing or has been cleaned up.
                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] No OperatorBarrierHandler found for {receiverKeyForLog} to handle barrier {barrierProto.CheckpointId}. This might be a sink or a source if barriers are routed unexpectedly.");
                        }
                    }
                    else // Regular data record
                    {
                        // Existing logic for data records using DataReceiverRegistry:
                        string receiverKey = $"{dataRecord.TargetJobVertexId}_{dataRecord.TargetSubtaskIndex}"; // ensure receiverKey is defined here too
                        if (DataReceiverRegistry.RecordProcessors.TryGetValue(receiverKey, out var processor))
                        {
                            try
                            {
                                DataExchangeMetrics.RecordsReceived.Add(1); // Count only actual data records
                                await processor(dataRecord.TargetJobVertexId, dataRecord.TargetSubtaskIndex, dataRecord.Payload.ToByteArray());
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[DataExchangeService-{_taskManagerId}] Error in processor for {receiverKey} processing data: {ex.Message}");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[DataExchangeService-{_taskManagerId}] No record processor registered for {receiverKey}. Discarding data record.");
                        }
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
