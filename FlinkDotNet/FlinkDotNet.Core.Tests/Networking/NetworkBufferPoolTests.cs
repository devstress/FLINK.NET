using FlinkDotNet.Core.Networking;

namespace FlinkDotNet.Core.Tests.Networking
{
    public class NetworkBufferPoolTests
    {
        [Fact]
        public void Constructor_ValidParameters_InitializesCorrectly()
        {
            // Arrange & Act
            using var pool = new NetworkBufferPool(10, 1024);

            // Assert
            Assert.Equal(1024, pool.BufferSegmentSize);
            Assert.Equal(10, pool.TotalPoolBuffers);
            Assert.Equal(10, pool.AvailablePoolBuffers);
        }

        [Fact]
        public void Constructor_ZeroOrNegativeTotalSegments_ThrowsArgumentOutOfRangeException()
        {
            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new NetworkBufferPool(0, 1024));
            Assert.Throws<ArgumentOutOfRangeException>(() => new NetworkBufferPool(-1, 1024));
        }

        [Fact]
        public void Constructor_ZeroOrNegativeSegmentSize_ThrowsArgumentOutOfRangeException()
        {
            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new NetworkBufferPool(10, 0));
            Assert.Throws<ArgumentOutOfRangeException>(() => new NetworkBufferPool(10, -1));
        }

        [Fact]
        public void RequestMemorySegment_HasAvailableSegments_ReturnsSegment()
        {
            // Arrange
            using var pool = new NetworkBufferPool(5, 1024);

            // Act
            var segment = pool.RequestMemorySegment();

            // Assert
            Assert.NotNull(segment);
            Assert.True(segment.Length >= 1024); // ArrayPool might return larger buffers
            Assert.Equal(4, pool.AvailablePoolBuffers);
        }

        [Fact]
        public void RequestMemorySegment_NoAvailableSegments_ReturnsNull()
        {
            // Arrange
            using var pool = new NetworkBufferPool(2, 1024);
            var segment1 = pool.RequestMemorySegment();
            var segment2 = pool.RequestMemorySegment();

            // Act
            var segment3 = pool.RequestMemorySegment();

            // Assert
            Assert.NotNull(segment1);
            Assert.NotNull(segment2);
            Assert.Null(segment3);
            Assert.Equal(0, pool.AvailablePoolBuffers);
        }

        [Fact]
        public void RequestMemorySegment_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var pool = new NetworkBufferPool(5, 1024);
            pool.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => pool.RequestMemorySegment());
        }

        [Fact]
        public void RecycleMemorySegment_ValidSegment_AddsBackToPool()
        {
            // Arrange
            using var pool = new NetworkBufferPool(5, 1024);
            var segment = pool.RequestMemorySegment();
            Assert.NotNull(segment);
            Assert.Equal(4, pool.AvailablePoolBuffers);

            // Act
            pool.RecycleMemorySegment(segment);

            // Assert
            Assert.Equal(5, pool.AvailablePoolBuffers);
        }

        [Fact]
        public void RecycleMemorySegment_NullSegment_ThrowsArgumentNullException()
        {
            // Arrange
            using var pool = new NetworkBufferPool(5, 1024);

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => pool.RecycleMemorySegment(null!));
        }

        [Fact]
        public void RecycleMemorySegment_SegmentTooSmall_DoesNotAddToPool()
        {
            // Arrange
            using var pool = new NetworkBufferPool(5, 1024);
            var smallSegment = new byte[512]; // Smaller than segment size

            // Act
            pool.RecycleMemorySegment(smallSegment);

            // Assert
            Assert.Equal(5, pool.AvailablePoolBuffers); // Should remain unchanged
        }

        [Fact]
        public void RecycleMemorySegment_PoolDisposed_ReturnsToArrayPool()
        {
            // Arrange
            var pool = new NetworkBufferPool(5, 1024);
            var segment = pool.RequestMemorySegment();
            Assert.NotNull(segment);
            pool.Dispose();

            // Act & Assert
            // Should not throw - segment should be returned to ArrayPool
            pool.RecycleMemorySegment(segment);
        }

        [Fact]
        public void RequestBuffer_HasAvailableSegments_ReturnsNetworkBuffer()
        {
            // Arrange
            using var pool = new NetworkBufferPool(5, 1024);

            // Act
            var buffer = pool.RequestBuffer(512);

            // Assert
            Assert.NotNull(buffer);
            Assert.True(buffer.Capacity >= 1024);
            Assert.Equal(4, pool.AvailablePoolBuffers);
        }

        [Fact]
        public void RequestBuffer_ZeroOrNegativeMinCapacity_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            using var pool = new NetworkBufferPool(5, 1024);

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => pool.RequestBuffer(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => pool.RequestBuffer(-1));
        }

        [Fact]
        public void RequestBuffer_MinCapacityLargerThanSegmentSize_ReturnsBuffer()
        {
            // Arrange
            using var pool = new NetworkBufferPool(5, 1024);

            // Act
            var buffer = pool.RequestBuffer(2048); // Larger than segment size

            // Assert
            // Should still return a buffer even if it doesn't meet the minimum capacity requirement
            Assert.NotNull(buffer);
        }

        [Fact]
        public void RequestBuffer_NoAvailableSegments_ReturnsNull()
        {
            // Arrange
            using var pool = new NetworkBufferPool(1, 1024);
            var buffer1 = pool.RequestBuffer(1);
            Assert.NotNull(buffer1);

            // Act
            var buffer2 = pool.RequestBuffer(1);

            // Assert
            Assert.Null(buffer2);
        }

        [Fact]
        public void RequestBuffer_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var pool = new NetworkBufferPool(5, 1024);
            pool.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => pool.RequestBuffer(1));
        }

        [Fact]
        public void ReturnBuffer_ValidBuffer_DisposesBuffer()
        {
            // Arrange
            using var pool = new NetworkBufferPool(5, 1024);
            var buffer = pool.RequestBuffer(1);
            Assert.NotNull(buffer);
            Assert.Equal(4, pool.AvailablePoolBuffers);

            // Act
            pool.ReturnBuffer(buffer);

            // Assert
            Assert.Equal(5, pool.AvailablePoolBuffers);
            // Buffer should be disposed after return
            Assert.Throws<ObjectDisposedException>(() => buffer.GetMemory());
        }

        [Fact]
        public void ReturnBuffer_NullBuffer_ThrowsArgumentNullException()
        {
            // Arrange
            using var pool = new NetworkBufferPool(5, 1024);

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => pool.ReturnBuffer(null!));
        }

        [Fact]
        public void ReturnBuffer_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var pool = new NetworkBufferPool(5, 1024);
            var buffer = pool.RequestBuffer(1);
            Assert.NotNull(buffer);
            pool.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => pool.ReturnBuffer(buffer));
        }

        [Fact]
        public void RequestAndRecycleFlow_MultipleOperations_MaintainsPoolState()
        {
            // Arrange
            using var pool = new NetworkBufferPool(3, 1024);
            var segments = new List<byte[]>();

            // Act - Request all segments
            for (int i = 0; i < 3; i++)
            {
                var segment = pool.RequestMemorySegment();
                Assert.NotNull(segment);
                segments.Add(segment);
            }
            Assert.Equal(0, pool.AvailablePoolBuffers);

            // Act - Recycle half
            pool.RecycleMemorySegment(segments[0]);
            pool.RecycleMemorySegment(segments[1]);
            Assert.Equal(2, pool.AvailablePoolBuffers);

            // Act - Request again
            var newSegment = pool.RequestMemorySegment();
            Assert.NotNull(newSegment);
            Assert.Equal(1, pool.AvailablePoolBuffers);

            // Act - Recycle remaining
            pool.RecycleMemorySegment(segments[2]);
            pool.RecycleMemorySegment(newSegment);

            // Assert
            Assert.Equal(3, pool.AvailablePoolBuffers);
        }

        [Fact]
        public void Dispose_ReturnsAllSegmentsToArrayPool()
        {
            // Arrange
            var pool = new NetworkBufferPool(5, 1024);
            pool.RequestMemorySegment();
            pool.RequestMemorySegment();
            Assert.Equal(3, pool.AvailablePoolBuffers);

            // Act
            pool.Dispose();

            // Assert
            // Should not throw - all segments returned to ArrayPool
            Assert.Equal(0, pool.AvailablePoolBuffers);
        }

        [Fact]
        public void Dispose_CalledMultipleTimes_DoesNotThrow()
        {
            // Arrange
            var pool = new NetworkBufferPool(5, 1024);

            // Act & Assert
            pool.Dispose();
            pool.Dispose(); // Should not throw
            pool.Dispose(); // Should not throw
        }
    }
}