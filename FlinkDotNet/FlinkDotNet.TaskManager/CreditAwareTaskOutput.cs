using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Serializers;
using FlinkDotNet.Proto.Internal; // For OperatorOutput, DataExchangeService, UpstreamPayload, DownstreamPayload, DataRecord
using Grpc.Core;
using Grpc.Net.Client; // For GrpcChannel

namespace FlinkDotNet.TaskManager
{
    public class CreditAwareTaskOutput // : ICollector<object> or similar if it were to be used in allCollectors directly for non-networked part
    {
        private readonly string _sourceJobVertexId;
        private readonly int _sourceSubtaskIndex;
        private readonly OperatorOutput _outputInfo; // From TDD
        private readonly ITypeSerializer<object> _serializer; // Assuming object for now
        private readonly string _targetTmEndpoint;

        private GrpcChannel? _channel;
        private DataExchangeService.DataExchangeServiceClient? _client;
        private AsyncDuplexStreamingCall<UpstreamPayload, DownstreamPayload>? _call;

        private int _availableCredits = 0;
        private readonly ConcurrentQueue<DataRecord> _sendQueue = new ConcurrentQueue<DataRecord>();
        private readonly ManualResetEventSlim _sendSignal = new ManualResetEventSlim(false); // Start unsignaled, set when new data or new credits

        private Task? _processSendQueueTask;
        private Task? _processDownstreamMessagesTask;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly string _identifier;

        public CreditAwareTaskOutput(
            string sourceJobVertexId,
            int sourceSubtaskIndex,
            OperatorOutput outputInfo,
            ITypeSerializer<object> serializer, // TODO: Make this generic <T> for the specific output type
            string targetTmEndpoint)
        {
            _sourceJobVertexId = sourceJobVertexId;
            _sourceSubtaskIndex = sourceSubtaskIndex;
            _outputInfo = outputInfo;
            _serializer = serializer;
            _targetTmEndpoint = targetTmEndpoint;
            _identifier = $"CreditAwareOutput-{_sourceJobVertexId}_{_sourceSubtaskIndex}->{_outputInfo.TargetVertexId}_{_outputInfo.TargetSpecificSubtaskIndex}";

            Console.WriteLine($"[{_identifier}] Created for target {_targetTmEndpoint}");
        }

        public async Task ConnectAsync(CancellationToken externalCt) // Allow external cancellation to be linked
        {
            if (_call != null) {
                Console.WriteLine($"[{_identifier}] ConnectAsync called but already connected or connecting.");
                return;
            }

            // Link externalCt to internal _cts if external cancellation should also stop this output
            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCt, _cts.Token);

            Console.WriteLine($"[{_identifier}] Connecting to {_targetTmEndpoint}...");
            try
            {
                _channel = GrpcChannel.ForAddress(_targetTmEndpoint, new GrpcChannelOptions
                {
                    // Optional: Configure Keepalive, SSL, etc.
                });
                _client = new DataExchangeService.DataExchangeServiceClient(_channel);
                _call = _client.ExchangeData(cancellationToken: linkedCts.Token); // Use linked token

                _processDownstreamMessagesTask = Task.Run(() => ProcessDownstreamMessagesAsync(linkedCts.Token));
                _processSendQueueTask = Task.Run(() => ProcessSendQueueAsync(linkedCts.Token));

                Console.WriteLine($"[{_identifier}] Successfully connected. Background tasks started.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_identifier}] Connection to {_targetTmEndpoint} failed: {ex.Message}");
                _call = null;
                _channel = null;
                _cts.Cancel(); // Ensure background tasks don't run if connection failed initially
                throw;
            }
        }

        public void SendRecord(DataRecord record)
        {
            if (_cts.IsCancellationRequested)
            {
                // Console.WriteLine($"[{_identifier}] SendRecord called but output is closing/closed. Record for {record.TargetJobVertexId} dropped.");
                return;
            }
            _sendQueue.Enqueue(record);
            _sendSignal.Set();
        }

        private async Task ProcessSendQueueAsync(CancellationToken ct)
        {
            Console.WriteLine($"[{_identifier}] Send queue processing task started.");
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        _sendSignal.Wait(ct);
                    }
                    catch (OperationCanceledException) { break; } // Exit if wait is cancelled

                    if (ct.IsCancellationRequested) break;

                    bool processedDuringLock = false;
                    lock (_sendSignal) // Lock to ensure atomicity of credit check, dequeue, and signal reset
                    {
                        if (ct.IsCancellationRequested) break;

                        if (_availableCredits > 0 && _sendQueue.TryDequeue(out DataRecord? dataRecord))
                        {
                            Interlocked.Decrement(ref _availableCredits);
                            processedDuringLock = true; // We are about to process this item

                            // Fire and forget the send operation to release the lock quickly.
                            // Manage the task if needed for shutdown or error propagation.
                            _ = Task.Run(async () => {
                                try
                                {
                                    if (_call == null || ct.IsCancellationRequested) {
                                        Console.WriteLine($"[{_identifier}] Send error: gRPC call is null or task cancelled. Re-queueing record for {dataRecord.TargetJobVertexId}.");
                                        _sendQueue.Enqueue(dataRecord);
                                        Interlocked.Increment(ref _availableCredits);
                                        _sendSignal.Set(); // Re-signal due to re-queue and credit return
                                        return;
                                    }
                                    // Console.WriteLine($"[{_identifier}] Sending record. Credits before: {_availableCredits + 1}, After: {_availableCredits}. Queue: {_sendQueue.Count}");
                                    UpstreamPayload upstreamPayload = new UpstreamPayload { Record = dataRecord };
                                    await _call.RequestStream.WriteAsync(upstreamPayload, ct);
                                }
                                catch (OperationCanceledException) when (ct.IsCancellationRequested) {
                                    Console.WriteLine($"[{_identifier}] Send operation cancelled while writing. Re-queueing record for {dataRecord.TargetJobVertexId}.");
                                    _sendQueue.Enqueue(dataRecord);
                                    Interlocked.Increment(ref _availableCredits);
                                }
                                catch (Exception ex) {
                                    Console.WriteLine($"[{_identifier}] Send error during WriteAsync for {dataRecord.TargetJobVertexId}: {ex.Message}. Re-queueing.");
                                    _sendQueue.Enqueue(dataRecord);
                                    Interlocked.Increment(ref _availableCredits);
                                    // Consider marking _call as potentially unhealthy or triggering reconnect
                                    // For now, we assume ProcessDownstreamMessagesAsync or a keepalive would detect broken pipe.
                                }
                                finally {
                                    _sendSignal.Set(); // After send attempt (success or fail with re-queue), re-evaluate send loop.
                                }
                            });
                        }

                        // After attempting to process, decide if signal needs to be reset or remain set.
                        if (_availableCredits > 0 && !_sendQueue.IsEmpty)
                        {
                           if(!processedDuringLock) _sendSignal.Set(); // If we didn't process an item this cycle but could, keep signal set.
                                                                       // If processedDuringLock, the Task.Run's finally block will set it.
                        }
                        else
                        {
                            _sendSignal.Reset(); // No credits or queue empty
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"[{_identifier}] Send queue processing task cancelled (outer loop).");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_identifier}] Critical error in send queue processing task: {ex.Message} {ex.StackTrace}");
            }
            finally
            {
                Console.WriteLine($"[{_identifier}] Send queue processing task finished.");
            }
        }

        private async Task ProcessDownstreamMessagesAsync(CancellationToken ct)
        {
            Console.WriteLine($"[{_identifier}] Downstream message processing task started.");
            if (_call == null)
            {
                 Console.WriteLine($"[{_identifier}] Cannot process downstream messages, gRPC call is null (connection failed?).");
                 return;
            }

            try
            {
                await foreach (var downstreamPayload in _call.ResponseStream.ReadAllAsync(ct))
                {
                    if (downstreamPayload.PayloadCase == DownstreamPayload.PayloadOneofCase.Credits)
                    {
                        int newCredits = downstreamPayload.Credits.CreditsGranted;
                        if (newCredits <=0) continue; // Ignore non-positive credit updates

                        Interlocked.Add(ref _availableCredits, newCredits);
                        // Console.WriteLine($"[{_identifier}] Received {newCredits} credits. Total available: {_availableCredits}. Queue size: {_sendQueue.Count}");
                        if (!_sendQueue.IsEmpty) // Only signal if there's something to send
                        {
                            _sendSignal.Set();
                        }
                    }
                }
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled || (ex.InnerException is OperationCanceledException oce && oce.CancellationToken == ct) )
            {
                Console.WriteLine($"[{_identifier}] Downstream processing cancelled (stream ended by server or token).");
            }
            catch (OperationCanceledException)
            {
                 Console.WriteLine($"[{_identifier}] Downstream processing task explicitly cancelled via CancellationToken.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_identifier}] Error processing downstream messages: {ex.Message} {ex.StackTrace}");
                if (!_cts.IsCancellationRequested) _cts.Cancel(); // Signal other parts to shut down due to error
            }
            finally
            {
                 Console.WriteLine($"[{_identifier}] Downstream message processing loop ended.");
                 if (!_cts.IsCancellationRequested) _cts.Cancel(); // Ensure other tasks know this one is done.
                 _sendSignal.Set(); // Ensure send queue doesn't block indefinitely if this task ends.
            }
        }

        public async Task CloseAsync()
        {
            Console.WriteLine($"[{_identifier}] CloseAsync called.");
            if (!_cts.IsCancellationRequested)
            {
                _cts.Cancel();
            }
            _sendSignal.Set(); // Wake up send queue task so it can observe cancellation and terminate

            // Attempt to gracefully complete the request stream
            if (_call?.RequestStream != null)
            {
                try
                {
                    await _call.RequestStream.CompleteAsync();
                    Console.WriteLine($"[{_identifier}] Request stream completed.");
                }
                catch (InvalidOperationException ioe) when (ioe.Message.Contains("already completed")) {
                     Console.WriteLine($"[{_identifier}] Request stream was already completed.");
                }
                catch (RpcException rpce) when (rpce.StatusCode == StatusCode.Cancelled || rpce.StatusCode == StatusCode.Unavailable) {
                     Console.WriteLine($"[{_identifier}] Request stream completion failed due to cancellation or unavailable: {rpce.Status}");
                }
                catch (Exception ex) { Console.WriteLine($"[{_identifier}] Error completing request stream: {ex.GetType().Name} - {ex.Message}."); }
            }

            // Wait for background tasks with a timeout
            var sendQueueTaskCompletion = _processSendQueueTask ?? Task.CompletedTask;
            var downstreamTaskCompletion = _processDownstreamMessagesTask ?? Task.CompletedTask;

            try
            {
                await Task.WhenAll(sendQueueTaskCompletion, downstreamTaskCompletion).WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (TimeoutException)
            {
                Console.WriteLine($"[{_identifier}] Timeout waiting for background tasks to complete during CloseAsync.");
            }
            catch (Exception ex) // Catch any other exceptions from WhenAll
            {
                Console.WriteLine($"[{_identifier}] Exception waiting for background tasks during CloseAsync: {ex.Message}");
            }

            _call?.Dispose();
            if (_channel != null)
            {
                try {
                    // ShutdownAsync can hang if there are active calls that don't terminate.
                    // Since _call should be disposed or completed, this should be okay.
                    await Task.WhenAny(_channel.ShutdownAsync(), Task.Delay(TimeSpan.FromSeconds(2)));
                }
                catch (Exception ex) { Console.WriteLine($"[{_identifier}] Exception shutting down gRPC channel: {ex.Message}");}
            }

            _sendSignal.Dispose();
            _cts.Dispose();
            // _sendQueue.Clear(); // Not needed as ConcurrentQueue doesn't have Clear(). Items will be GC'd.
            Console.WriteLine($"[{_identifier}] Closed. Available credits at close: {_availableCredits}, Send queue items left: {_sendQueue.Count}");
        }
    }
}
#nullable disable
