using System;
using System.Collections.Concurrent;
using System.Threading;

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
        private readonly ConcurrentQueue<NetworkBuffer> _availableLocalBuffers = new ConcurrentQueue<NetworkBuffer>();
        private int _numLeasedSegmentsFromGlobal = 0;
        private bool _isDestroyed = false;

        // New fields for asynchronous requests and callbacks
        private readonly ConcurrentQueue<TaskCompletionSource<NetworkBuffer>> _pendingRequests = new ConcurrentQueue<TaskCompletionSource<NetworkBuffer>>();
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

        private void ReturnSegmentToLocalPool(NetworkBuffer buffer)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));

            lock (_lock)
            {
                if (_isDestroyed)
                {
                    // If this LocalBufferPool is destroyed, return the segment directly to the global pool.
                    _globalBufferPool.RecycleMemorySegment(buffer.UnderlyingBuffer);
                    // It's tricky to reliably decrement _numLeasedSegmentsFromGlobal here if multiple threads
                    // are returning segments after destruction, as the lock state might be contended.
                    // However, the primary goal is to return the segment to its origin (ArrayPool via global pool).
                    return;
                }

                // Check if the local pool is already over its desired maximum due to concurrent returns
                // or if it should recycle to global pool to maintain its quota.
                // The condition checks if keeping this buffer would exceed maxConfiguredSegments,
                // considering buffers currently available locally and those given out but still owned by this LBP.
                // Current total segments "owned" by LBP = _numLeasedSegmentsFromGlobal
                // Buffers currently "out on loan" from LBP = _numLeasedSegmentsFromGlobal - _availableLocalBuffers.Count
                // If we add this buffer back, new available = _availableLocalBuffers.Count + 1
                // We should recycle to global if _numLeasedSegmentsFromGlobal > _maxConfiguredSegments
                // AND if keeping it locally would still keep us over the max (this part is tricky)
                // Simpler: If we have _numLeasedSegmentsFromGlobal > _maxConfiguredSegments, it means we have an excess segment from global.
                // So, returning this one to global helps reduce that excess.
                if (_numLeasedSegmentsFromGlobal > _maxConfiguredSegments)
                {
                     _globalBufferPool.RecycleMemorySegment(buffer.UnderlyingBuffer);
                     Interlocked.Decrement(ref _numLeasedSegmentsFromGlobal);
                }
                else
                {
                     buffer.Reset(); // Prepare for reuse
                    _availableLocalBuffers.Enqueue(buffer);
                }

                // After successfully re-enqueuing or deciding not to (if over max),
                // check if there are pending requests that can now be fulfilled.
                // This check should happen regardless of whether the buffer was re-queued or returned to global,
                // as a slot might have "conceptually" freed up or a pending request could use this specific buffer.
                // However, if it was returned to global, we don't have a buffer instance to give to a pending request.
                // So, only fulfill pending if the buffer was re-enqueued locally.
                if (_availableLocalBuffers.Contains(buffer)) // Check if it was actually enqueued (not returned to global)
                {
                    if (_pendingRequests.TryDequeue(out TaskCompletionSource<NetworkBuffer>? tcsToFulfill))
                    {
                        // Dequeue the buffer we just enqueued to give to the pending request
                        if (_availableLocalBuffers.TryDequeue(out NetworkBuffer? dequeuedBufferForTcs))
                        {
                            if (tcsToFulfill.TrySetResult(dequeuedBufferForTcs))
                            {
                                Console.WriteLine($"[{_poolIdentifier}] Fulfilled pending buffer request.");
                                return; // Buffer given to a waiter
                            }
                            else
                            {
                                // TCS was already completed/cancelled, re-enqueue buffer
                                Console.WriteLine($"[{_poolIdentifier}] Pending request TCS already completed/cancelled. Re-enqueuing buffer.");
                                dequeuedBufferForTcs.Reset(); // Reset again as it was dequeued
                                _availableLocalBuffers.Enqueue(dequeuedBufferForTcs);
                                // Fall through to general callback if any, as this buffer is now "available" again.
                            }
                        }
                        else
                        {
                             // Should not happen if we just enqueued it and are under lock.
                             Console.WriteLine($"[{_poolIdentifier}] CRITICAL: Failed to dequeue buffer for pending request immediately after enqueuing.");
                             // Re-queue TCS if buffer not available.
                             _pendingRequests.Enqueue(tcsToFulfill); // Simplistic re-queue, might need better handling.
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

        public NetworkBuffer? RequestBuffer(int minCapacity = 0)
        {
            if (minCapacity > BufferSegmentSize)
            {
                 Console.WriteLine($"[{_poolIdentifier}] Warning: Requested buffer capacity {minCapacity} exceeds segment size {BufferSegmentSize}. Attempting to serve with a single segment.");
            }

            lock (_lock)
            {
                if (_isDestroyed) throw new ObjectDisposedException(nameof(LocalBufferPool), $"Pool {_poolIdentifier} is disposed.");

                if (_availableLocalBuffers.TryDequeue(out NetworkBuffer? buffer))
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
                        return new NetworkBuffer(segment, ReturnSegmentToLocalPool, 0, segment.Length);
                    }
                }
                // Console.WriteLine($"[{_poolIdentifier}] No buffer available immediately (local empty, global constraint).");
                return null;
            }
        }

        public async ValueTask<NetworkBuffer> RequestBufferAsync(CancellationToken cancellationToken = default)
        {
            TaskCompletionSource<NetworkBuffer> tcs;
            lock (_lock)
            {
                if (_isDestroyed) throw new ObjectDisposedException(nameof(LocalBufferPool), $"Pool {_poolIdentifier} is disposed.");

                if (_availableLocalBuffers.TryDequeue(out NetworkBuffer? buffer))
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
                        return new NetworkBuffer(segment, ReturnSegmentToLocalPool, 0, segment.Length);
                    }
                }

                // No buffer immediately available, queue the request
                // Console.WriteLine($"[{_poolIdentifier}] Async request: Queuing request. Pending: {_pendingRequests.Count + 1}");
                tcs = new TaskCompletionSource<NetworkBuffer>(TaskCreationOptions.RunContinuationsAsynchronously);
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

        public void ReturnBuffer(NetworkBuffer buffer)
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
                while (_availableLocalBuffers.TryDequeue(out NetworkBuffer? bufferToRecycle))
                {
                    _globalBufferPool.RecycleMemorySegment(bufferToRecycle.UnderlyingBuffer);
                    Interlocked.Decrement(ref _numLeasedSegmentsFromGlobal); // Decrement as we are returning one leased segment
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
#nullable disable
