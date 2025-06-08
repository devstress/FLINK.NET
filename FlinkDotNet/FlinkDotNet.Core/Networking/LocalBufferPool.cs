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
        private readonly Action<LocalBufferPool, int>? _onBufferReturnedToLocalPoolCallback; // Made readonly
        private readonly string _poolIdentifier; // Made readonly
        private readonly ConcurrentQueue<INetworkBuffer> _availableLocalBuffers = new ConcurrentQueue<INetworkBuffer>();
        private int _numLeasedSegmentsFromGlobal; // Removed explicit default
        private bool _isDestroyed; // Removed explicit default

        // New fields for asynchronous requests and callbacks
        private readonly ConcurrentQueue<TaskCompletionSource<INetworkBuffer>> _pendingRequests = new ConcurrentQueue<TaskCompletionSource<INetworkBuffer>>();

        private bool _disposed = false; // For IDisposable pattern


        public LocalBufferPool(
            NetworkBufferPool globalBufferPool,
            int minRequiredSegments,
            int maxConfiguredSegments,
            Action<LocalBufferPool, int>? onBufferReturnedCallback = null,
            string poolIdentifier = "Unknown")
        {
            ArgumentNullException.ThrowIfNull(globalBufferPool);
            _globalBufferPool = globalBufferPool;

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

        private void ReturnSegmentToLocalPool(NetworkBuffer buffer)
        {
            ArgumentNullException.ThrowIfNull(buffer);

            lock (_lock)
            {
                if (_isDestroyed || _disposed) // Check _disposed as well
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
                                // Console.WriteLine($"[{_poolIdentifier}] Fulfilled pending buffer request."); // S125: Removed
                                return;
                            }
                            else
                            {
                                // Console.WriteLine($"[{_poolIdentifier}] Pending request TCS already completed/cancelled. Re-enqueuing buffer."); // S125: Removed
                                ((NetworkBuffer)dequeuedBufferForTcs).Reset();
                                _availableLocalBuffers.Enqueue(dequeuedBufferForTcs);
                            }
                        }
                        else
                        {
                             // Console.WriteLine($"[{_poolIdentifier}] CRITICAL: Failed to dequeue buffer for pending request immediately after enqueuing."); // S125: Removed
                             _pendingRequests.Enqueue(tcsToFulfill); // Re-enqueue TCS if buffer couldn't be dequeued
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
                if (_isDestroyed || _disposed) throw new ObjectDisposedException(nameof(LocalBufferPool), $"Pool {_poolIdentifier} is disposed.");

                if (_availableLocalBuffers.TryDequeue(out INetworkBuffer? buffer))
                {
                    // Console.WriteLine($"[{_poolIdentifier}] Provided buffer from local queue."); // S125: Removed
                    return buffer;
                }

                // Try to get more from global pool if under max quota
                if (_numLeasedSegmentsFromGlobal < _maxConfiguredSegments)
                {
                    byte[]? segment = _globalBufferPool.RequestMemorySegment();
                    if (segment != null)
                    {
                        Interlocked.Increment(ref _numLeasedSegmentsFromGlobal);
                        // Console.WriteLine($"[{_poolIdentifier}] Leased new segment from global. Total leased: {_numLeasedSegmentsFromGlobal}"); // S125: Removed
                        return new NetworkBuffer(segment, ReturnSegmentToLocalPool, 0, segment.Length);
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
                if (_isDestroyed || _disposed) throw new ObjectDisposedException(nameof(LocalBufferPool), $"Pool {_poolIdentifier} is disposed.");

                if (_availableLocalBuffers.TryDequeue(out INetworkBuffer? buffer))
                {
                    // Console.WriteLine($"[{_poolIdentifier}] Async request: Provided buffer from local queue."); // S125: Removed
                    return buffer;
                }

                // This is the S1066 candidate. The logic is: if no local buffer, try to get from global if allowed.
                // If global fails or not allowed, then queue request. This sequence seems logical.
                // Merging `if (_numLeasedSegmentsFromGlobal < _maxConfiguredSegments)` with the outer lock doesn't simplify.
                // The cognitive complexity S3776 warning might be due to multiple exit points and nested conditions.
                // The current structure is relatively clear for a buffer pool: try local, try global, then wait.
                if (_numLeasedSegmentsFromGlobal < _maxConfiguredSegments)
                {
                    byte[]? segment = _globalBufferPool.RequestMemorySegment();
                    if (segment != null)
                    {
                        Interlocked.Increment(ref _numLeasedSegmentsFromGlobal);
                        // Console.WriteLine($"[{_poolIdentifier}] Async request: Leased new segment from global. Total leased: {_numLeasedSegmentsFromGlobal}"); // S125: Removed
                        return new NetworkBuffer(segment, ReturnSegmentToLocalPool, 0, segment.Length);
                    }
                }

                // No buffer immediately available, queue the request
                // Console.WriteLine($"[{_poolIdentifier}] Async request: Queuing request. Pending: {_pendingRequests.Count + 1}"); // S125: Removed
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
                 // Console.WriteLine($"[{_poolIdentifier}] Async buffer request was cancelled."); // S125: Removed
                // If TCS was cancelled but still in queue, subsequent TryDequeue in ReturnSegmentToLocalPool might find it.
                // Its TrySetResult would fail, and buffer re-enqueued. This is acceptable.
                throw;
            }
        }

        public void ReturnBuffer(INetworkBuffer buffer)
        {
            ArgumentNullException.ThrowIfNull(buffer);
            // The NetworkBuffer's Dispose method calls the _returnToPoolAction,
            // which is ReturnSegmentToLocalPool for buffers managed by this LBP.
            buffer.Dispose();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Dispose managed state (managed objects).
                    lock (_lock) // Ensure thread safety during dispose
                    {
                        if (_isDestroyed) return; // Already effectively disposed by a different path
                        _isDestroyed = true; // Mark as destroyed to stop normal operations

                        // Console.WriteLine($"[LocalBufferPool] Disposing. {_availableLocalBuffers.Count} available locally. {_numLeasedSegmentsFromGlobal} leased from global."); // S125: Removed
                        int recycledToGlobalCount = 0;
                        while (_pendingRequests.TryDequeue(out var tcs))
                        {
                            tcs.TrySetCanceled(); // Cancel any pending requests
                        }

                        while (_availableLocalBuffers.TryDequeue(out INetworkBuffer? bufferToRecycle))
                        {
                            // Assuming NetworkBuffer.UnderlyingBuffer gives access to the byte[]
                            _globalBufferPool.RecycleMemorySegment(((NetworkBuffer)bufferToRecycle).UnderlyingBuffer);
                            Interlocked.Decrement(ref _numLeasedSegmentsFromGlobal);
                            recycledToGlobalCount++;
                        }
                        // Console.WriteLine($"[LocalBufferPool] Returned {recycledToGlobalCount} segments to global pool during dispose."); // S125: Removed
                        // Console.WriteLine($"[LocalBufferPool] Remaining _numLeasedSegmentsFromGlobal (should be 0 if all buffers returned to LBP before dispose): {_numLeasedSegmentsFromGlobal}"); // S125: Removed
                    }
                    // TODO: Consider if _globalBufferPool itself needs unsubscription or notification
                    // Example: _globalBufferPool.DestroyBufferPool(this); // If such a mechanism exists
                }

                // Free unmanaged resources (unmanaged objects) and override a finalizer below.
                // Set large fields to null.
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Optional: Finalizer if directly owning unmanaged resources not handled by NetworkBuffer/NetworkBufferPool
        // ~LocalBufferPool()
        // {
        //     Dispose(false);
        // }
    }
}
