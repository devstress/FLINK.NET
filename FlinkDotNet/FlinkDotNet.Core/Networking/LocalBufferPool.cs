using System;
using System.Collections.Concurrent;
using System.Threading;
using FlinkDotNet.Core.Abstractions.Networking; // Added for INetworkBufferPool and INetworkBuffer

namespace FlinkDotNet.Core.Networking
{
    /// <summary>
    /// Manages a local collection of NetworkBuffer instances, drawing memory segments
    /// from a global NetworkBufferPool.
    /// </summary>
    public class LocalBufferPool : INetworkBufferPool, IDisposable
    {
        private readonly object _lock = new object();
        private readonly NetworkBufferPool _globalBufferPool;
        private readonly int _minRequiredSegments;
        private readonly int _maxConfiguredSegments;
        private readonly ConcurrentQueue<INetworkBuffer> _availableLocalBuffers = new ConcurrentQueue<INetworkBuffer>();
        private int _numLeasedSegmentsFromGlobal = 0;
        private bool _isDestroyed = false;

        // New fields for asynchronous requests and callbacks
        private readonly ConcurrentQueue<TaskCompletionSource<INetworkBuffer>> _pendingRequests = new ConcurrentQueue<TaskCompletionSource<INetworkBuffer>>();
        private Action<LocalBufferPool, int>? _onBufferReturnedToLocalPoolCallback;
        private string _poolIdentifier;


        public LocalBufferPool(
            NetworkBufferPool globalBufferPool,
            int minRequiredSegments,
            int maxConfiguredSegments,
            Action<LocalBufferPool, int>? onBufferReturnedCallback = null,
            string poolIdentifier = "Unknown")
        {
            _globalBufferPool = globalBufferPool ?? throw new ArgumentNullException(nameof(globalBufferPool));

            if (minRequiredSegments <= 0) throw new ArgumentOutOfRangeException(nameof(minRequiredSegments), "Must be positive.");
            if (maxConfiguredSegments < minRequiredSegments) throw new ArgumentOutOfRangeException(nameof(maxConfiguredSegments), "Must be greater than or equal to minRequiredSegments.");

            _minRequiredSegments = minRequiredSegments;
            _maxConfiguredSegments = maxConfiguredSegments;
            _onBufferReturnedToLocalPoolCallback = onBufferReturnedCallback;
            _poolIdentifier = poolIdentifier;

            // Initially, attempt to request and populate minRequiredSegments
            for (int i = 0; i < _minRequiredSegments; i++)
            {
                byte[]? segment = _globalBufferPool.RequestMemorySegment();
                if (segment != null)
                {
                    // The NetworkBuffer's recycle action is set to this pool's ReturnSegmentToLocalPool
                    _availableLocalBuffers.Enqueue(new NetworkBuffer(segment, ReturnSegmentToLocalPool, 0, segment.Length));
                    Interlocked.Increment(ref _numLeasedSegmentsFromGlobal);
                }
                else
                {
                    Console.WriteLine($"[LocalBufferPool] Warning: Could not pre-allocate all required minimum segments. Requested {_minRequiredSegments}, allocated {i}. Global pool might be depleted.");
                    break;
                }
            }
        }

        private void ReturnSegmentToLocalPool(NetworkBuffer buffer) // Reverted parameter type to NetworkBuffer
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));

            lock (_lock)
            {
                if (_isDestroyed)
                {
                    _globalBufferPool.RecycleMemorySegment(buffer.UnderlyingBuffer);
                    return;
                }

                if (_numLeasedSegmentsFromGlobal > _maxConfiguredSegments)
                {
                     _globalBufferPool.RecycleMemorySegment(buffer.UnderlyingBuffer);
                     Interlocked.Decrement(ref _numLeasedSegmentsFromGlobal);
                }
                else
                {
                     buffer.Reset();
                    _availableLocalBuffers.Enqueue(buffer);
                }

                // Cast to INetworkBuffer for Contains check if _availableLocalBuffers is INetworkBuffer
                // Or ensure _availableLocalBuffers stores NetworkBuffer if this path is taken.
                // Given other changes, _availableLocalBuffers is ConcurrentQueue<INetworkBuffer>
                // So, buffer (which is NetworkBuffer) is implicitly convertible.
                if (_availableLocalBuffers.Contains(buffer))
                {
                    if (_pendingRequests.TryDequeue(out TaskCompletionSource<INetworkBuffer>? tcsToFulfill))
                    {
                        if (_availableLocalBuffers.TryDequeue(out INetworkBuffer? dequeuedBufferForTcs))
                        {
                            if (tcsToFulfill.TrySetResult(dequeuedBufferForTcs))
                            {
                                Console.WriteLine($"[{_poolIdentifier}] Fulfilled pending buffer request.");
                                return;
                            }
                            else
                            {
                                Console.WriteLine($"[{_poolIdentifier}] Pending request TCS already completed/cancelled. Re-enqueuing buffer.");
                                ((NetworkBuffer)dequeuedBufferForTcs).Reset();
                                _availableLocalBuffers.Enqueue(dequeuedBufferForTcs);
                            }
                        }
                        else
                        {
                             Console.WriteLine($"[{_poolIdentifier}] CRITICAL: Failed to dequeue buffer for pending request immediately after enqueuing.");
                             _pendingRequests.Enqueue(tcsToFulfill);
                        }
                    }
                }

                // If no pending request took the buffer (or it was returned to global and a slot conceptually freed up)
                // Invoke the general availability callback.
                // The callback is invoked if a buffer became available *locally* or if a segment was returned to global *reducing pressure*.
                // The '1' signifies one buffer processed/returned.
                _onBufferReturnedToLocalPoolCallback?.Invoke(this, 1);
            }
        }

        // INetworkBufferPool implementation
        public int BufferSegmentSize => _globalBufferPool.BufferSegmentSize;

        public int TotalPoolBuffers => _maxConfiguredSegments; // This LBP aims to manage up to this many.

        public int AvailablePoolBuffers => _availableLocalBuffers.Count;

        public INetworkBuffer? RequestBuffer(int minCapacity = 0)
        {
            if (minCapacity > BufferSegmentSize)
            {
                 Console.WriteLine($"[{_poolIdentifier}] Warning: Requested buffer capacity {minCapacity} exceeds segment size {BufferSegmentSize}. Attempting to serve with a single segment.");
            }

            lock (_lock)
            {
                if (_isDestroyed) throw new ObjectDisposedException(nameof(LocalBufferPool), $"Pool {_poolIdentifier} is disposed.");

                if (_availableLocalBuffers.TryDequeue(out INetworkBuffer? buffer))
                {
                    // Console.WriteLine($"[{_poolIdentifier}] Provided buffer from local queue.");
                    return buffer;
                }

                // Try to get more from global pool if under max quota
                if (_numLeasedSegmentsFromGlobal < _maxConfiguredSegments)
                {
                    byte[]? segment = _globalBufferPool.RequestMemorySegment();
                    if (segment != null)
                    {
                        Interlocked.Increment(ref _numLeasedSegmentsFromGlobal);
                        Console.WriteLine($"[{_poolIdentifier}] Leased new segment from global. Total leased: {_numLeasedSegmentsFromGlobal}");
                        return new NetworkBuffer(segment, ReturnSegmentToLocalPool, 0, segment.Length); // Pass matching Action<NetworkBuffer>
                    }
                }
                return null;
            }
        }

        public async ValueTask<INetworkBuffer> RequestBufferAsync(CancellationToken cancellationToken = default)
        {
            TaskCompletionSource<INetworkBuffer> tcs;
            lock (_lock)
            {
                if (_isDestroyed) throw new ObjectDisposedException(nameof(LocalBufferPool), $"Pool {_poolIdentifier} is disposed.");

                if (_availableLocalBuffers.TryDequeue(out INetworkBuffer? buffer))
                {
                    // Console.WriteLine($"[{_poolIdentifier}] Async request: Provided buffer from local queue.");
                    return buffer;
                }

                if (_numLeasedSegmentsFromGlobal < _maxConfiguredSegments)
                {
                    byte[]? segment = _globalBufferPool.RequestMemorySegment();
                    if (segment != null)
                    {
                        Interlocked.Increment(ref _numLeasedSegmentsFromGlobal);
                        Console.WriteLine($"[{_poolIdentifier}] Async request: Leased new segment from global. Total leased: {_numLeasedSegmentsFromGlobal}");
                        return new NetworkBuffer(segment, ReturnSegmentToLocalPool, 0, segment.Length); // Pass matching Action<NetworkBuffer>
                    }
                }

                // No buffer immediately available, queue the request
                // Console.WriteLine($"[{_poolIdentifier}] Async request: Queuing request. Pending: {_pendingRequests.Count + 1}");
                tcs = new TaskCompletionSource<INetworkBuffer>(TaskCreationOptions.RunContinuationsAsynchronously);
                _pendingRequests.Enqueue(tcs);
            }

            // Await outside the lock
            using var registration = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
            try
            {
                return await tcs.Task.ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                 Console.WriteLine($"[{_poolIdentifier}] Async buffer request was cancelled.");
                // If TCS was cancelled but still in queue, subsequent TryDequeue in ReturnSegmentToLocalPool might find it.
                // Its TrySetResult would fail, and buffer re-enqueued. This is acceptable.
                throw;
            }
        }

        public void ReturnBuffer(INetworkBuffer buffer)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            // The NetworkBuffer's Dispose method calls the _returnToPoolAction,
            // which is ReturnSegmentToLocalPool for buffers managed by this LBP.
            buffer.Dispose();
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_isDestroyed) return;
                _isDestroyed = true;

                Console.WriteLine($"[LocalBufferPool] Disposing. {_availableLocalBuffers.Count} available locally. {_numLeasedSegmentsFromGlobal} leased from global.");
                int recycledToGlobalCount = 0;
                while (_availableLocalBuffers.TryDequeue(out INetworkBuffer? bufferToRecycle))
                {
                    _globalBufferPool.RecycleMemorySegment(((NetworkBuffer)bufferToRecycle).UnderlyingBuffer);
                    Interlocked.Decrement(ref _numLeasedSegmentsFromGlobal);
                    recycledToGlobalCount++;
                }
                 Console.WriteLine($"[LocalBufferPool] Returned {recycledToGlobalCount} segments to global pool during dispose.");
                 Console.WriteLine($"[LocalBufferPool] Remaining _numLeasedSegmentsFromGlobal (should be 0 if all buffers returned to LBP before dispose): {_numLeasedSegmentsFromGlobal}");
            }
            // Unregister from global pool if it maintains a list of local pools (advanced feature)
            // _globalBufferPool.DestroyBufferPool(this);
            GC.SuppressFinalize(this);
        }
    }
}
