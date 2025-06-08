using System.Threading.Tasks; // For ValueTask if RequestBufferAsync is preferred

namespace FlinkDotNet.Core.Abstractions.Networking
{
    /// <summary>
    /// Defines a pool for managing NetworkBuffer instances to reduce GC pressure
    /// and manage memory for network operations.
    /// </summary>
    public interface INetworkBufferPool
    {
        /// <summary>
        /// Gets the configured size of individual buffer segments managed by this pool.
        /// </summary>
        int BufferSegmentSize { get; }

        /// <summary>
        /// Gets the total number of buffer segments this pool is configured to manage.
        /// </summary>
        int TotalPoolBuffers { get; }

        /// <summary>
        /// Gets the current number of available (free) buffer segments in the pool.
        /// This count should be thread-safe.
        /// </summary>
        int AvailablePoolBuffers { get; }

        /// <summary>
        /// Requests a NetworkBuffer from the pool.
        /// This method might block if no buffers are available and the pool is configured to wait,
        /// or it might return null or throw an exception if unable to satisfy immediately.
        /// </summary>
        /// <param name="minCapacity">The minimum capacity required for the buffer.
        /// The pool will try to provide a buffer of at least this size, typically its standard BufferSegmentSize.</param>
        /// <returns>A NetworkBuffer instance, or null if no buffer is available and the pool is non-blocking.</returns>
        INetworkBuffer? RequestBuffer(int minCapacity = 0); // minCapacity might often be just BufferSegmentSize

        // Alternative async version if blocking is handled by async waits:

        /// <summary>
        /// Returns a NetworkBuffer (and its underlying byte array) to the pool.
        /// </summary>
        /// <param name="buffer">The NetworkBuffer to return.</param>
        void ReturnBuffer(INetworkBuffer buffer);
    }
}
