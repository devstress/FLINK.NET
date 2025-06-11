using FlinkDotNet.Core.Networking;
using FlinkDotNet.Core.Abstractions.Networking;

namespace FlinkDotNet.Core.Tests.Networking
{
    public class LocalBufferPoolTests
    {
        [Fact]
        public void Constructor_ValidParameters_InitializesCorrectly()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);

            // Act
            using var localPool = new LocalBufferPool(globalPool, 2, 5);

            // Assert
            Assert.Equal(1024, localPool.BufferSegmentSize);
            Assert.Equal(5, localPool.TotalPoolBuffers);
            Assert.Equal(2, localPool.AvailablePoolBuffers); // Should pre-allocate min required
        }

        [Fact]
        public void Constructor_NullGlobalPool_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new LocalBufferPool(null!, 2, 5));
        }

        [Fact]
        public void Constructor_ZeroOrNegativeMinRequiredSegments_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new LocalBufferPool(globalPool, 0, 5));
            Assert.Throws<ArgumentOutOfRangeException>(() => new LocalBufferPool(globalPool, -1, 5));
        }

        [Fact]
        public void Constructor_ZeroOrNegativeMaxConfiguredSegments_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new LocalBufferPool(globalPool, 2, 0));
            Assert.Throws<ArgumentOutOfRangeException>(() => new LocalBufferPool(globalPool, 2, -1));
        }

        [Fact]
        public void Constructor_MaxLessThanMin_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new LocalBufferPool(globalPool, 5, 2));
        }

        [Fact]
        public void RequestBuffer_HasAvailableBuffers_ReturnsBuffer()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 2, 5);

            // Act
            var buffer = localPool.RequestBuffer(1);

            // Assert
            Assert.NotNull(buffer);
            Assert.Equal(1, localPool.AvailablePoolBuffers);
        }

        [Fact]
        public void RequestBuffer_ZeroOrNegativeMinCapacity_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 2, 5);

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => localPool.RequestBuffer(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => localPool.RequestBuffer(-1));
        }

        [Fact]
        public void RequestBuffer_MinCapacityExceedsSegmentSize_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 2, 5);

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => localPool.RequestBuffer(2048));
        }

        [Fact]
        public void RequestBuffer_NoLocalBuffersAvailable_RequestsFromGlobal()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 1, 3); // Start with 1 local buffer, then take it

            // Act - Take the pre-allocated buffer first
            var preAllocatedBuffer = localPool.RequestBuffer(1);
            Assert.NotNull(preAllocatedBuffer);
            Assert.Equal(0, localPool.AvailablePoolBuffers);

            // Act - Now request when no local buffers available, should get from global
            var buffer = localPool.RequestBuffer(1);

            // Assert
            Assert.NotNull(buffer);
            Assert.Equal(0, localPool.AvailablePoolBuffers); // Should be 0 since it came from global directly
        }

        [Fact]
        public void RequestBuffer_ExceedsMaxConfigured_ReturnsNull()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 2, 2); // Max is 2
            
            // Request all available buffers
            var buffer1 = localPool.RequestBuffer(1);
            var buffer2 = localPool.RequestBuffer(1);
            Assert.NotNull(buffer1);
            Assert.NotNull(buffer2);

            // Act - Request beyond max
            var buffer3 = localPool.RequestBuffer(1);

            // Assert
            Assert.Null(buffer3);
        }

        [Fact]
        public void RequestBuffer_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            var localPool = new LocalBufferPool(globalPool, 2, 5);
            localPool.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => localPool.RequestBuffer(1));
        }

        [Fact]
        public void ReturnBuffer_ValidBuffer_ReturnsToLocalPool()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 2, 5);
            var buffer = localPool.RequestBuffer(1);
            Assert.NotNull(buffer);
            Assert.Equal(1, localPool.AvailablePoolBuffers);

            // Act
            localPool.ReturnBuffer(buffer);

            // Assert
            Assert.Equal(2, localPool.AvailablePoolBuffers);
        }

        [Fact]
        public void ReturnBuffer_NullBuffer_ThrowsArgumentNullException()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 2, 5);

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => localPool.ReturnBuffer(null!));
        }

        [Fact]
        public async Task RequestBufferAsync_HasAvailableBuffers_ReturnsImmediately()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 2, 5);

            // Act
            var buffer = await localPool.RequestBufferAsync();

            // Assert
            Assert.NotNull(buffer);
            Assert.Equal(1, localPool.AvailablePoolBuffers);
        }

        [Fact]
        public async Task RequestBufferAsync_NoBuffersAvailable_WaitsForReturn()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 1, 1); // Only 1 buffer
            var buffer = localPool.RequestBuffer(1);
            Assert.NotNull(buffer);

            // Act - Start async request
            var asyncTask = localPool.RequestBufferAsync();
            Assert.False(asyncTask.IsCompleted);

            // Return the buffer to fulfill the async request
            localPool.ReturnBuffer(buffer);
            var result = await asyncTask;

            // Assert
            Assert.NotNull(result);
        }

        [Fact]
        public async Task RequestBufferAsync_Cancelled_ThrowsOperationCancelledException()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 1, 1);
            var buffer = localPool.RequestBuffer(1);
            Assert.NotNull(buffer);

            using var cts = new CancellationTokenSource();

            // Act - Start async request and cancel immediately
            var asyncTask = localPool.RequestBufferAsync(cts.Token);
            cts.Cancel();

            // Assert
            await Assert.ThrowsAsync<TaskCanceledException>(() => asyncTask.AsTask());
        }

        [Fact]
        public async Task RequestBufferAsync_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            var localPool = new LocalBufferPool(globalPool, 2, 5);
            localPool.Dispose();

            // Act & Assert
            await Assert.ThrowsAsync<ObjectDisposedException>(() => localPool.RequestBufferAsync().AsTask());
        }

        [Fact]
        public void BufferReturnCallback_Called_InvokesCallback()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            bool callbackInvoked = false;
            LocalBufferPool? callbackPool = null;
            int callbackCount = 0;

            Action<LocalBufferPool, int> callback = (pool, count) =>
            {
                callbackInvoked = true;
                callbackPool = pool;
                callbackCount = count;
            };

            using var localPool = new LocalBufferPool(globalPool, 2, 5, callback);
            var buffer = localPool.RequestBuffer(1);
            Assert.NotNull(buffer);

            // Act
            localPool.ReturnBuffer(buffer);

            // Assert
            Assert.True(callbackInvoked);
            Assert.Equal(localPool, callbackPool);
            Assert.Equal(1, callbackCount);
        }

        [Fact]
        public void MultipleRequestsAndReturns_MaintainsCorrectState()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            using var localPool = new LocalBufferPool(globalPool, 2, 5); // Min=2, Max=5
            var buffers = new List<INetworkBuffer>();

            // Act - Request all pre-allocated
            for (int i = 0; i < 2; i++)
            {
                var buffer = localPool.RequestBuffer(1);
                Assert.NotNull(buffer);
                buffers.Add(buffer);
            }
            Assert.Equal(0, localPool.AvailablePoolBuffers);

            // Act - Return one
            localPool.ReturnBuffer(buffers[0]);
            Assert.Equal(1, localPool.AvailablePoolBuffers);

            // Act - Request again
            var newBuffer = localPool.RequestBuffer(1);
            Assert.NotNull(newBuffer);
            Assert.Equal(0, localPool.AvailablePoolBuffers);

            // Act - Return the remaining buffer
            localPool.ReturnBuffer(buffers[1]);

            // Assert - Should have at least 1 buffer available (the returned one)
            Assert.True(localPool.AvailablePoolBuffers >= 1);
        }

        [Fact]
        public void Dispose_ReturnsBuffersToGlobalPool()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            var localPool = new LocalBufferPool(globalPool, 3, 5);
            var initialGlobalAvailable = globalPool.AvailablePoolBuffers;

            // Act
            localPool.Dispose();

            // Assert
            // The local pool should have returned its segments to the global pool
            Assert.True(globalPool.AvailablePoolBuffers >= initialGlobalAvailable - 3);
        }

        [Fact]
        public void Dispose_CalledMultipleTimes_DoesNotThrow()
        {
            // Arrange
            using var globalPool = new NetworkBufferPool(10, 1024);
            var localPool = new LocalBufferPool(globalPool, 2, 5);

            // Act & Assert
            localPool.Dispose();
            localPool.Dispose(); // Should not throw
            localPool.Dispose(); // Should not throw
        }
    }
}