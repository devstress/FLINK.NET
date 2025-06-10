using System.Buffers; // For ArrayPool<byte>
using System.Collections.Concurrent; // For ConcurrentBag or other thread-safe collections
using FlinkDotNet.Core.Abstractions.Networking; // Added for INetworkBufferPool

namespace FlinkDotNet.Core.Networking
{
    /// <summary>
    /// A basic implementation of INetworkBufferPool primarily using ArrayPool<byte>.Shared
    /// Manages a pool of fixed-size memory segments (byte arrays).
    /// It pre-allocates a fixed number of segments and provides them to consumers (e.g., LocalBufferPool).
    /// </summary>
    public class NetworkBufferPool : INetworkBufferPool, IDisposable
    {
        private readonly int _segmentSize;
        private readonly int _totalSegments;
        private readonly ConcurrentQueue<byte[]> _availableSegments;
        private bool _disposed; // CA1805: Removed explicit default

        public NetworkBufferPool(int totalSegments, int segmentSize)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(totalSegments); // CA1512
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(segmentSize);   // CA1512

            _totalSegments = totalSegments;
            _segmentSize = segmentSize;
            _availableSegments = new ConcurrentQueue<byte[]>();

            for (int i = 0; i < _totalSegments; i++)
            {
                // Rent segments from ArrayPool.Shared. These will be returned on Dispose
                // or when segments are recycled after the pool is disposed.
                var segment = ArrayPool<byte>.Shared.Rent(_segmentSize);
                _availableSegments.Enqueue(segment);
            }
        }

        public int BufferSegmentSize => _segmentSize;
        public int TotalPoolBuffers => _totalSegments;
        public int AvailablePoolBuffers => _availableSegments.Count;

        /// <summary>
        /// Requests a memory segment from the pool.
        /// </summary>
        /// <returns>A byte array segment, or null if no segments are available.</returns>
        public byte[]? RequestMemorySegment()
        {
            ObjectDisposedException.ThrowIf(_disposed, this); // CA1513

            if (_availableSegments.TryDequeue(out byte[]? segment))
            {
                return segment;
            }
            return null;
        }

        /// <summary>
        /// Recycles a memory segment back into the pool.
        /// </summary>
        /// <param name="segment">The segment to recycle.</param>
        public void RecycleMemorySegment(byte[] segment)
        {
            ArgumentNullException.ThrowIfNull(segment);

            if (_disposed)
            {
                ArrayPool<byte>.Shared.Return(segment);
                return;
            }

            // Basic check: ensure segment is of the correct size for this pool,
            // though ArrayPool might return slightly larger buffers than requested.
            // We are interested if it's smaller, which would be an error.
            if (segment.Length < _segmentSize)
            {
                ArrayPool<byte>.Shared.Return(segment);
                return;
            }

            _availableSegments.Enqueue(segment);
        }

        // Implementation of INetworkBufferPool methods (Revised Task 4)
        public INetworkBuffer? RequestBuffer(int minCapacity = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this); // CA1513
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minCapacity); // CA1512

            // minCapacity can be used to decide if multiple segments are needed,
            // but for now, NetworkBuffer wraps a single segment from this pool.
            // If minCapacity > _segmentSize, this simple request will fail to meet it unless segment is coincidentally large enough.
            if (minCapacity > _segmentSize)
            {
                // This pool provides fixed-size segments. Requesting larger than segment size is not
                // directly supported by requesting a single segment. A LocalBufferPool might aggregate.
            }

            byte[]? segment = RequestMemorySegment();
            if (segment != null)
            {
                // The NetworkBuffer's recycle action is now this pool's RecycleMemorySegment.
                return new NetworkBuffer(segment, buffer => RecycleMemorySegment(buffer.UnderlyingBuffer), 0, segment.Length); // Capacity is full segment length
            }
            return null;
        }

        public void ReturnBuffer(INetworkBuffer buffer)
        {
            ObjectDisposedException.ThrowIf(_disposed, this); // CA1513
            ArgumentNullException.ThrowIfNull(buffer); // CA1510
            // The NetworkBuffer's Dispose method should call the recycle action,
            // which is now directly RecycleMemorySegment.
            // Calling buffer.Dispose() is the standard way to ensure recycling.
            buffer.Dispose();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Dispose managed state (managed objects).
                    // Return all currently available segments back to ArrayPool.Shared
                    int returnedCount = 0;
                    while (_availableSegments.TryDequeue(out byte[]? segment))
                    {
                        ArrayPool<byte>.Shared.Return(segment);
                        returnedCount++;
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

        // Optional: Finalizer if this class directly owns unmanaged resources.
    }
}
