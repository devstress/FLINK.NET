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
        private readonly int _maxConfiguredSegments;
        private readonly Action<LocalBufferPool, int>? _onBufferReturnedToLocalPoolCallback; // Made readonly
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

            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minRequiredSegments); // CA1512
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxConfiguredSegments); // CA1512
            if (maxConfiguredSegments < minRequiredSegments) throw new ArgumentOutOfRangeException(nameof(maxConfiguredSegments), "Must be greater than or equal to minRequiredSegments.");

            _maxConfiguredSegments = maxConfiguredSegments;
            _onBufferReturnedToLocalPoolCallback = onBufferReturnedCallback;
            // poolIdentifier parameter retained for potential future diagnostics

            // Initially, attempt to request and populate minRequiredSegments
            for (int i = 0; i < minRequiredSegments; i++)
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
                if (_isDestroyed || _disposed)
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

                if (_availableLocalBuffers.Contains(buffer) && TryFulfillPendingRequest())
                {
                    return;
                }

                _onBufferReturnedToLocalPoolCallback?.Invoke(this, 1);
            }
        }

        private bool TryFulfillPendingRequest()
        {
            if (!_pendingRequests.TryDequeue(out var tcs))
            {
                return false;
            }

            if (_availableLocalBuffers.TryDequeue(out var dequeuedBuffer))
            {
                if (tcs.TrySetResult(dequeuedBuffer))
                {
                    return true;
                }

                ((NetworkBuffer)dequeuedBuffer).Reset();
                _availableLocalBuffers.Enqueue(dequeuedBuffer);
            }
            else
            {
                _pendingRequests.Enqueue(tcs);
            }

            return false;
        }

        // INetworkBufferPool implementation
        public int BufferSegmentSize => _globalBufferPool.BufferSegmentSize;

        public int TotalPoolBuffers => _maxConfiguredSegments; // This LBP aims to manage up to this many.

        public int AvailablePoolBuffers => _availableLocalBuffers.Count;

        public INetworkBuffer? RequestBuffer(int minCapacity = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minCapacity); // Caller info automatically provided

            if (minCapacity > BufferSegmentSize)
            {
                throw new ArgumentOutOfRangeException(nameof(minCapacity), "Requested capacity exceeds buffer segment size.");
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
            return await tcs.Task.ConfigureAwait(false);
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
