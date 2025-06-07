using System;
using System.Buffers; // For ArrayPool<byte>
using System.Collections.Concurrent; // For ConcurrentBag or other thread-safe collections
using System.Threading;

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
        private bool _disposed = false;

        public NetworkBufferPool(int totalSegments, int segmentSize)
        {
            if (totalSegments <= 0) throw new ArgumentOutOfRangeException(nameof(totalSegments));
            if (segmentSize <= 0) throw new ArgumentOutOfRangeException(nameof(segmentSize));

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
            if (_disposed) throw new ObjectDisposedException(nameof(NetworkBufferPool));

            if (_availableSegments.TryDequeue(out byte[]? segment))
            {
                return segment;
            }
            // Console.WriteLine("[NetworkBufferPool] No memory segments available."); // Optional: for high-frequency logging
            return null;
        }

        /// <summary>
        /// Recycles a memory segment back into the pool.
        /// </summary>
        /// <param name="segment">The segment to recycle.</param>
        public void RecycleMemorySegment(byte[] segment)
        {
            if (segment == null) throw new ArgumentNullException(nameof(segment));

            // Basic check: ensure segment is of the correct size for this pool,
            // though ArrayPool might return slightly larger buffers than requested.
            // We are interested if it's smaller, which would be an error.
            if (segment.Length < _segmentSize)
            {
                 // This indicates a logic error or a segment not originating from this pool's Rent calls.
                 Console.WriteLine($"[NetworkBufferPool] Warning: Attempted to recycle segment of size {segment.Length}, which is smaller than pool's segment size {_segmentSize}. Segment will not be re-queued. This may lead to buffer leaks if it was from this pool, or is an error if from elsewhere.");
                 // To be safe, if it's not from our pool (or seems corrupted), don't re-add.
                 // If it was from ArrayPool.Shared, it should still be returned there if we don't re-enqueue.
                 // However, if the pool is disposed, we MUST return it to ArrayPool.
                 if (_disposed) ArrayPool<byte>.Shared.Return(segment); // Ensure it's returned if pool is dead
                 return;
            }

            if (_disposed)
            {
                // If the pool is disposed, any segment being recycled should go directly back to ArrayPool.Shared
                ArrayPool<byte>.Shared.Return(segment);
                return;
            }

            _availableSegments.Enqueue(segment);
        }

        // Implementation of INetworkBufferPool methods (Revised Task 4)
        public NetworkBuffer? RequestBuffer(int minCapacity = 0)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(NetworkBufferPool));

            // minCapacity can be used to decide if multiple segments are needed,
            // but for now, NetworkBuffer wraps a single segment from this pool.
            // If minCapacity > _segmentSize, this simple request will fail to meet it unless segment is coincidentally large enough.
            if (minCapacity > _segmentSize)
            {
                // This pool provides fixed-size segments. Requesting larger than segment size
                // is not directly supported by requesting a single segment.
                // A LocalBufferPool might aggregate, or this indicates a wrong pool is being asked.
                Console.WriteLine($"[NetworkBufferPool] Warning: Requested buffer capacity {minCapacity} exceeds segment size {_segmentSize}. Attempting to serve with a single segment.");
                // Fallthrough to attempt serving with one segment. NetworkBuffer will throw if capacity is insufficient.
            }

            byte[]? segment = RequestMemorySegment();
            if (segment != null)
            {
                // The NetworkBuffer's recycle action is now this pool's RecycleMemorySegment.
                return new NetworkBuffer(segment, buffer => RecycleMemorySegment(buffer.UnderlyingBuffer), 0, segment.Length); // Capacity is full segment length
            }
            return null;
        }

        public void ReturnBuffer(NetworkBuffer buffer)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            // The NetworkBuffer's Dispose method should call the recycle action,
            // which is now directly RecycleMemorySegment.
            // Calling buffer.Dispose() is the standard way to ensure recycling.
            buffer.Dispose();
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            // Return all currently available segments back to ArrayPool.Shared
            int returnedCount = 0;
            while (_availableSegments.TryDequeue(out byte[]? segment))
            {
                ArrayPool<byte>.Shared.Return(segment);
                returnedCount++;
            }
            Console.WriteLine($"[NetworkBufferPool] Disposed. Returned {returnedCount} segments to ArrayPool.Shared.");

            // Segments that were "leased out" (i.e., requested via RequestMemorySegment or RequestBuffer
            // and not yet recycled) will be returned to ArrayPool.Shared by their holders (NetworkBuffer instances)
            // when they call RecycleMemorySegment, which will see that the pool is disposed.
            GC.SuppressFinalize(this);
        }
    }
}
#nullable disable
