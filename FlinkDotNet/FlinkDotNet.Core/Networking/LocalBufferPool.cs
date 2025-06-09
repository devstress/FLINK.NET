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

        private bool _disposed; // CA1805: For IDisposable pattern


        public LocalBufferPool(
            NetworkBufferPool globalBufferPool,
            int minRequiredSegments,
            int maxConfiguredSegments,
            Action<LocalBufferPool, int>? onBufferReturnedCallback = null,
            string poolIdentifier = "Unknown")
        {
            ArgumentNullException.ThrowIfNull(globalBufferPool);
            ArgumentNullException.ThrowIfNull(poolIdentifier); // Assuming this was added from previous CA1510 task
            _globalBufferPool = globalBufferPool;

            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minRequiredSegments, nameof(minRequiredSegments)); // CA1512
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxConfiguredSegments, nameof(maxConfiguredSegments)); // CA1512
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
                                return;
                            }
                            else
                            {
                                ((NetworkBuffer)dequeuedBufferForTcs).Reset();
                                _availableLocalBuffers.Enqueue(dequeuedBufferForTcs);
                            }
                        }
                        else
                        {
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
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minCapacity, nameof(minCapacity)); // CA1512 (Corrected to ThrowIfNegativeOrZero)

            if (minCapacity > BufferSegmentSize)
            {
            }

            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this); // CA1513 (using _disposed, assumes _isDestroyed implies _disposed)

                if (_availableLocalBuffers.TryDequeue(out INetworkBuffer? buffer))
                {
                    return buffer;
                }

                // Try to get more from global pool if under max quota
                if (_numLeasedSegmentsFromGlobal < _maxConfiguredSegments)
                {
                    byte[]? segment = _globalBufferPool.RequestMemorySegment();
                    if (segment != null)
                    {
                        Interlocked.Increment(ref _numLeasedSegmentsFromGlobal);
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
                ObjectDisposedException.ThrowIf(_disposed, this); // CA1513

                if (_availableLocalBuffers.TryDequeue(out INetworkBuffer? buffer))
                {
                    return buffer;
                }

                if (_numLeasedSegmentsFromGlobal < _maxConfiguredSegments)
                {
                    byte[]? segment = _globalBufferPool.RequestMemorySegment();
                    if (segment != null)
                    {
                        Interlocked.Increment(ref _numLeasedSegmentsFromGlobal);
                        return new NetworkBuffer(segment, ReturnSegmentToLocalPool, 0, segment.Length);
                    }
                }

                // No buffer immediately available, queue the request
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

                        while (_pendingRequests.TryDequeue(out var tcs))
                        {
                            tcs.TrySetCanceled(); // Cancel any pending requests
                        }

                        while (_availableLocalBuffers.TryDequeue(out INetworkBuffer? bufferToRecycle))
                        {
                            // Assuming NetworkBuffer.UnderlyingBuffer gives access to the byte[]
                            _globalBufferPool.RecycleMemorySegment(((NetworkBuffer)bufferToRecycle).UnderlyingBuffer);
                            Interlocked.Decrement(ref _numLeasedSegmentsFromGlobal);
                        }
                    }
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

    }
}
