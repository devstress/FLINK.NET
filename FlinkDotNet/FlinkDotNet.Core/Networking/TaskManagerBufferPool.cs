#nullable enable
using System;
using System.Buffers; // For ArrayPool<byte>
using System.Collections.Concurrent; // For ConcurrentBag or other thread-safe collections
using System.Threading;

namespace FlinkDotNet.Core.Networking
{
    /// <summary>
    /// A basic implementation of INetworkBufferPool primarily using ArrayPool<byte>.Shared
    /// for managing byte arrays. This version simplifies by not strictly enforcing TotalPoolBuffers
    /// via ArrayPool directly, but rather as a conceptual limit if we were to manage NetworkBuffer objects
    /// in a dedicated collection. For this implementation, credits and external logic would primarily gate usage.
    /// </summary>
    public class TaskManagerBufferPool : INetworkBufferPool, IDisposable
    {
        public int BufferSegmentSize { get; }
        public int TotalPoolBuffers { get; } // This is more of a conceptual target for this implementation.

        // For a pool that strictly manages NetworkBuffer objects (not just byte[] from ArrayPool):
        // private readonly ConcurrentBag<NetworkBuffer> _pool;
        // private readonly Func<byte[], Action<NetworkBuffer>, NetworkBuffer> _bufferFactory;

        private int _availableBuffersCount; // Approximate count if relying on ArrayPool sizing

        // This implementation will directly rent from ArrayPool<byte>.Shared when a buffer is requested
        // and return to it. The "pool" of NetworkBuffer objects themselves is not strictly managed here,
        // reducing complexity for this example. A more advanced pool would manage NetworkBuffer instances.

        /// <summary>
        /// Initializes a new instance of the TaskManagerBufferPool.
        /// </summary>
        /// <param name="bufferSegmentSize">The standard size for buffers rented from ArrayPool.</param>
        /// <param name="totalBuffersConfig">Conceptual total number of buffers the system aims to use.
        /// Actual enforcement relies on ArrayPool behavior and credit system.</param>
        public TaskManagerBufferPool(int bufferSegmentSize = 65536, int totalBuffersConfig = 1024) // e.g. 64KB
        {
            if (bufferSegmentSize <= 0) throw new ArgumentOutOfRangeException(nameof(bufferSegmentSize));
            if (totalBuffersConfig <= 0) throw new ArgumentOutOfRangeException(nameof(totalBuffersConfig));

            BufferSegmentSize = bufferSegmentSize;
            TotalPoolBuffers = totalBuffersConfig; // Conceptual limit
            _availableBuffersCount = TotalPoolBuffers; // Initial assumption

            // _pool = new ConcurrentBag<NetworkBuffer>();
            // _bufferFactory = (bytes, returnAction) => new NetworkBuffer(bytes, nb => ReturnBufferInternal(nb));
            // Pre-populate if managing NetworkBuffer objects:
            // for (int i = 0; i < TotalPoolBuffers; i++) { _pool.Add(_bufferFactory(new byte[BufferSegmentSize], null!)); }
        }

        public int AvailablePoolBuffers => Volatile.Read(ref _availableBuffersCount); // Thread-safe read

        public NetworkBuffer? RequestBuffer(int minCapacity = 0)
        {
            // This simplified version doesn't block if ArrayPool can't provide.
            // A real pool might have blocking or limited retries.
            // minCapacity is used to ensure the requested buffer is at least this size.
            // ArrayPool.Rent typically gives a buffer of at least the requested size, often larger.
            int sizeToRent = Math.Max(minCapacity, BufferSegmentSize);
            byte[] rentedArray;
            try
            {
                rentedArray = ArrayPool<byte>.Shared.Rent(sizeToRent);
            }
            catch (OutOfMemoryException) // Or other allocation exceptions from ArrayPool under extreme pressure
            {
                Console.WriteLine("[TaskManagerBufferPool] ArrayPool.Shared.Rent failed to allocate. System under memory pressure.");
                return null;
            }

            // Conceptually decrement available count. This is tricky with ArrayPool.Shared
            // as its size is not directly controlled by this class instance alone.
            // This count becomes more of an estimate or a gate for the credit system.
            Interlocked.Decrement(ref _availableBuffersCount);

            return new NetworkBuffer(rentedArray, buffer => ReturnBufferInternal(buffer), 0, 0);
        }

        private void ReturnBufferInternal(NetworkBuffer networkBuffer)
        {
            if (networkBuffer?.UnderlyingBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(networkBuffer.UnderlyingBuffer, clearArray: false); // Flink often clears buffers
                Interlocked.Increment(ref _availableBuffersCount);
                // networkBuffer.Invalidate(); // Mark NetworkBuffer instance as unusable after returning its array
            }
        }

        public void ReturnBuffer(NetworkBuffer buffer)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            // The NetworkBuffer's Dispose method should call the _returnToPoolAction,
            // which in turn calls ReturnBufferInternal.
            // If called directly, ensure it's idempotent or handle if already disposed.
            buffer.Dispose(); // This will trigger the ReturnBufferInternal via the action.
        }

        public void Dispose()
        {
            // If we were managing NetworkBuffer instances in a ConcurrentBag and pre-allocating,
            // we might clear the bag here.
            // Since we rely on ArrayPool<byte>.Shared, there's no pool owned by this instance to clear.
            // However, if there were any outstanding NetworkBuffer objects created by this pool
            // that haven't been disposed, their finalizers (if any) or app shutdown would handle it.
            // For this example, Dispose doesn't do much beyond standard IDisposable pattern.
            GC.SuppressFinalize(this);
        }
    }
}
#nullable disable
