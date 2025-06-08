using System;
using System.IO; // For Stream, MemoryStream
using FlinkDotNet.Core.Abstractions.Networking;

namespace FlinkDotNet.Core.Networking
{
    /// <summary>
    /// Represents a segment of memory, typically pooled, used for network I/O.
    /// It provides stream-based access for reading and writing, and manages its return to a pool via IDisposable.
    /// </summary>
    public sealed class NetworkBuffer : INetworkBuffer
    {
        /// <summary>
        /// The underlying byte array from a pool.
        /// </summary>
        public byte[] UnderlyingBuffer { get; private set; }

        /// <summary>
        /// The offset within the UnderlyingBuffer where valid data starts.
        /// For buffers obtained from ArrayPool, this is typically 0.
        /// </summary>
        public int DataOffset { get; private set; } // Usually 0 if buffer is exclusively for this NetworkBuffer

        /// <summary>
        /// The length of the valid data currently stored in the UnderlyingBuffer.
        /// </summary>
        public int DataLength { get; private set; }

        /// <summary>
        /// Gets the total capacity of the underlying buffer segment.
        /// </summary>
        public int Capacity => UnderlyingBuffer.Length;

        /// <summary>
        /// Gets or sets a flag indicating whether this buffer contains a checkpoint barrier.
        /// This needs to be set by the producer of the buffer (e.g., NetworkedCollector).
        /// </summary>
        public bool IsBarrierPayload { get; private set; } // Made setter private, to be set via constructor or SetBarrierInfo

        /// <summary>
        /// Gets the Checkpoint ID if this buffer represents a barrier.
        /// </summary>
        public long CheckpointId { get; private set; }

        /// <summary>
        /// Gets the timestamp of the checkpoint if this buffer represents a barrier.
        /// </summary>
        public long CheckpointTimestamp { get; private set; }

        private readonly Action<NetworkBuffer>? _returnToPoolAction;
        private bool _isDisposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="NetworkBuffer"/> class.
        /// </summary>
        /// <param name="rentedBuffer">The byte array rented from a pool.</param>
        /// <param name="returnToPoolAction">The action to call to return this buffer to its pool.</param>
        /// <param name="initialDataOffset">The offset where data starts in the buffer (usually 0).</param>
        /// <param name="initialDataLength">The initial length of data in the buffer (usually 0 for writing).</param>
        /// <param name="isBarrier">Indicates if this buffer is a checkpoint barrier.</param>
        /// <param name="checkpointId">The ID of the checkpoint if it's a barrier.</param>
        /// <param name="checkpointTimestamp">The timestamp of the checkpoint if it's a barrier.</param>
        public NetworkBuffer(
            byte[] rentedBuffer,
            Action<NetworkBuffer>? returnToPoolAction,
            int initialDataOffset = 0,
            int initialDataLength = 0,
            bool isBarrier = false,
            long checkpointId = 0,
            long checkpointTimestamp = 0)
        {
            UnderlyingBuffer = rentedBuffer ?? throw new ArgumentNullException(nameof(rentedBuffer));
            _returnToPoolAction = returnToPoolAction;
            DataOffset = initialDataOffset;
            DataLength = initialDataLength;

            IsBarrierPayload = isBarrier;
            if (IsBarrierPayload)
            {
                CheckpointId = checkpointId;
                CheckpointTimestamp = checkpointTimestamp;
            }
            else
            {
                CheckpointId = 0;
                CheckpointTimestamp = 0;
            }
        }

        /// <summary>
        /// Sets or clears checkpoint barrier information for this buffer.
        /// </summary>
        /// <param name="isBarrier">True if this buffer should be marked as a barrier, false otherwise.</param>
        /// <param name="checkpointId">The checkpoint ID, used if isBarrier is true.</param>
        /// <param name="checkpointTimestamp">The checkpoint timestamp, used if isBarrier is true.</param>
        public void SetBarrierInfo(bool isBarrier, long checkpointId = 0, long checkpointTimestamp = 0)
        {
            IsBarrierPayload = isBarrier;
            if (IsBarrierPayload)
            {
                CheckpointId = checkpointId;
                CheckpointTimestamp = checkpointTimestamp;
            }
            else
            {
                CheckpointId = 0;
                CheckpointTimestamp = 0;
            }
        }

        /// <summary>
        /// Gets a Memory<byte> slice representing the valid data portion of the buffer.
        /// </summary>
        public Memory<byte> GetMemory()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NetworkBuffer));
            return new Memory<byte>(UnderlyingBuffer, DataOffset, DataLength);
        }

        /// <summary>
        /// Gets a ReadOnlyMemory<byte> slice representing the valid data portion of the buffer.
        /// </summary>
        public ReadOnlyMemory<byte> GetReadOnlyMemory()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NetworkBuffer));
            return new ReadOnlyMemory<byte>(UnderlyingBuffer, DataOffset, DataLength);
        }

        /// <summary>
        /// Provides a Stream for writing data into this buffer.
        /// The stream starts at the current DataOffset and is limited by the buffer's total capacity.
        /// DataLength will be updated as data is written to the stream.
        /// </summary>
        /// <returns>A MemoryStream configured for writing.</returns>
        public Stream GetWriteStream()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NetworkBuffer));
            // Reset DataLength for writing, assuming fresh write or overwrite.
            // Or, if appending, current DataOffset + DataLength would be the start.
            // For simplicity, let's assume GetWriteStream prepares for a new payload.
            DataLength = 0;
            var stream = new MemoryStream(UnderlyingBuffer, DataOffset, Capacity - DataOffset, writable: true);

            // To update DataLength after writing, we can wrap the stream or rely on caller to set DataLength.
            // A simple way: caller calls SetDataLength after writing.
            // A more robust way: a custom stream wrapper.
            // For now, caller should call SetDataLength or use the stream's Position.
            return stream;
        }

        /// <summary>
        /// Provides a Stream for reading the valid data currently in this buffer.
        /// </summary>
        /// <returns>A MemoryStream configured for reading.</returns>
        public Stream GetReadStream()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NetworkBuffer));
            return new MemoryStream(UnderlyingBuffer, DataOffset, DataLength, writable: false);
        }

        /// <summary>
        /// Sets the length of the valid data in the buffer.
        /// Should be called after writing to the buffer, e.g., via GetWriteStream().
        /// </summary>
        /// <param name="length">The new data length.</param>
        /// <exception cref="ArgumentOutOfRangeException">If length is negative or exceeds buffer capacity from offset.</exception>
        public void SetDataLength(int length)
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NetworkBuffer));
            if (length < 0 || DataOffset + length > Capacity)
            {
                throw new ArgumentOutOfRangeException(nameof(length),
                    $"Length must be non-negative and not exceed buffer capacity from offset. Offset: {DataOffset}, Length: {length}, Capacity: {Capacity}");
            }
            DataLength = length;
        }

        /// <summary>
        /// Resets the buffer for reuse (typically for writing).
        /// Sets DataLength to 0 and IsBarrierPayload to false.
        /// </summary>
        public void Reset()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NetworkBuffer));
            DataLength = 0;
            IsBarrierPayload = false;
            CheckpointId = 0;
            CheckpointTimestamp = 0;
            // DataOffset usually remains fixed as it's about where the usable segment starts in UnderlyingBuffer.
        }

        /// <summary>
        /// Returns the buffer to its pool if a return action was provided.
        /// </summary>
        public void Dispose()
        {
            if (!_isDisposed)
            {
                _returnToPoolAction?.Invoke(this);
                _isDisposed = true;
                // To prevent accidental reuse of the UnderlyingBuffer if it's from ArrayPool,
                // it's good practice to "clear" it or make this NetworkBuffer instance unusable.
                // The _isDisposed flag helps with this.
                // UnderlyingBuffer = null; // Or Array.Empty<byte>(); if not handled by pool on return
            }
        }
    }
}
