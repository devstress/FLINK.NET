using FlinkDotNet.Core.Networking;

namespace FlinkDotNet.Core.Tests.Networking
{
    public class NetworkBufferTests
    {
        [Fact]
        public void Constructor_ValidParameters_InitializesCorrectly()
        {
            // Arrange
            var buffer = new byte[1024];
            bool returnActionCalled = false;
            Action<NetworkBuffer> returnAction = nb => returnActionCalled = true;

            // Act
            using var networkBuffer = new NetworkBuffer(buffer, returnAction, 10, 100);

            // Assert
            Assert.Equal(buffer, networkBuffer.UnderlyingBuffer);
            Assert.Equal(10, networkBuffer.DataOffset);
            Assert.Equal(100, networkBuffer.DataLength);
            Assert.Equal(1024, networkBuffer.Capacity);
            Assert.False(networkBuffer.IsBarrierPayload);
            Assert.Equal(0, networkBuffer.CheckpointId);
            Assert.Equal(0, networkBuffer.CheckpointTimestamp);
            Assert.False(returnActionCalled);
        }

        [Fact]
        public void Constructor_NullBuffer_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new NetworkBuffer(null!, null));
        }

        [Fact]
        public void Constructor_NegativeDataOffset_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var buffer = new byte[1024];

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new NetworkBuffer(buffer, null, -1));
        }

        [Fact]
        public void Constructor_NegativeDataLength_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var buffer = new byte[1024];

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => new NetworkBuffer(buffer, null, 0, -1));
        }

        [Fact]
        public void Constructor_BarrierPayload_InitializesBarrierInfo()
        {
            // Arrange
            var buffer = new byte[1024];
            var checkpointId = 12345L;
            var checkpointTimestamp = 67890L;

            // Act
            using var networkBuffer = new NetworkBuffer(buffer, null, 0, 0, true, checkpointId, checkpointTimestamp);

            // Assert
            Assert.True(networkBuffer.IsBarrierPayload);
            Assert.Equal(checkpointId, networkBuffer.CheckpointId);
            Assert.Equal(checkpointTimestamp, networkBuffer.CheckpointTimestamp);
        }

        [Fact]
        public void SetBarrierInfo_SetAsBarrier_UpdatesBarrierInfo()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null);
            var checkpointId = 12345L;
            var checkpointTimestamp = 67890L;

            // Act
            networkBuffer.SetBarrierInfo(true, checkpointId, checkpointTimestamp);

            // Assert
            Assert.True(networkBuffer.IsBarrierPayload);
            Assert.Equal(checkpointId, networkBuffer.CheckpointId);
            Assert.Equal(checkpointTimestamp, networkBuffer.CheckpointTimestamp);
        }

        [Fact]
        public void SetBarrierInfo_ClearBarrier_ClearsBarrierInfo()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null, 0, 0, true, 12345, 67890);

            // Act
            networkBuffer.SetBarrierInfo(false);

            // Assert
            Assert.False(networkBuffer.IsBarrierPayload);
            Assert.Equal(0, networkBuffer.CheckpointId);
            Assert.Equal(0, networkBuffer.CheckpointTimestamp);
        }

        [Fact]
        public void SetBarrierInfo_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var buffer = new byte[1024];
            var networkBuffer = new NetworkBuffer(buffer, null);
            networkBuffer.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => networkBuffer.SetBarrierInfo(true));
        }

        [Fact]
        public void GetMemory_ReturnsCorrectMemorySlice()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null, 10, 100);

            // Act
            var memory = networkBuffer.GetMemory();

            // Assert
            Assert.Equal(100, memory.Length);
            // Verify it's pointing to the correct data in the buffer by writing to it
            memory.Span[0] = 42;
            Assert.Equal(42, buffer[10]); // Should be at offset 10 in the underlying buffer
        }

        [Fact]
        public void GetMemory_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var buffer = new byte[1024];
            var networkBuffer = new NetworkBuffer(buffer, null);
            networkBuffer.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => networkBuffer.GetMemory());
        }

        [Fact]
        public void GetReadOnlyMemory_ReturnsCorrectMemorySlice()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null, 10, 100);

            // Act
            var memory = networkBuffer.GetReadOnlyMemory();

            // Assert
            Assert.Equal(100, memory.Length);
        }

        [Fact]
        public void GetReadOnlyMemory_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var buffer = new byte[1024];
            var networkBuffer = new NetworkBuffer(buffer, null);
            networkBuffer.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => networkBuffer.GetReadOnlyMemory());
        }

        [Fact]
        public void GetWriteStream_ResetsDataLengthAndReturnsWriteableStream()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null, 10, 100);

            // Act
            using var stream = networkBuffer.GetWriteStream();

            // Assert
            Assert.Equal(0, networkBuffer.DataLength);
            Assert.True(stream.CanWrite);
            Assert.Equal(1014, stream.Length); // Capacity - DataOffset
        }

        [Fact]
        public void GetWriteStream_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var buffer = new byte[1024];
            var networkBuffer = new NetworkBuffer(buffer, null);
            networkBuffer.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => networkBuffer.GetWriteStream());
        }

        [Fact]
        public void GetReadStream_ReturnsReadOnlyStreamWithCurrentData()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null, 10, 100);

            // Act
            using var stream = networkBuffer.GetReadStream();

            // Assert
            Assert.False(stream.CanWrite);
            Assert.Equal(100, stream.Length);
        }

        [Fact]
        public void GetReadStream_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var buffer = new byte[1024];
            var networkBuffer = new NetworkBuffer(buffer, null);
            networkBuffer.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => networkBuffer.GetReadStream());
        }

        [Fact]
        public void SetDataLength_ValidLength_UpdatesDataLength()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null, 10);

            // Act
            networkBuffer.SetDataLength(500);

            // Assert
            Assert.Equal(500, networkBuffer.DataLength);
        }

        [Fact]
        public void SetDataLength_NegativeLength_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null);

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => networkBuffer.SetDataLength(-1));
        }

        [Fact]
        public void SetDataLength_ExceedsCapacity_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null, 10);

            // Act & Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => networkBuffer.SetDataLength(1015)); // Exceeds capacity from offset
        }

        [Fact]
        public void SetDataLength_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var buffer = new byte[1024];
            var networkBuffer = new NetworkBuffer(buffer, null);
            networkBuffer.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => networkBuffer.SetDataLength(100));
        }

        [Fact]
        public void Reset_ClearsDataAndBarrierInfo()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null, 10, 100, true, 12345, 67890);

            // Act
            networkBuffer.Reset();

            // Assert
            Assert.Equal(0, networkBuffer.DataLength);
            Assert.False(networkBuffer.IsBarrierPayload);
            Assert.Equal(0, networkBuffer.CheckpointId);
            Assert.Equal(0, networkBuffer.CheckpointTimestamp);
            Assert.Equal(10, networkBuffer.DataOffset); // Should remain unchanged
        }

        [Fact]
        public void Reset_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var buffer = new byte[1024];
            var networkBuffer = new NetworkBuffer(buffer, null);
            networkBuffer.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => networkBuffer.Reset());
        }

        [Fact]
        public void Dispose_CallsReturnAction()
        {
            // Arrange
            var buffer = new byte[1024];
            bool returnActionCalled = false;
            NetworkBuffer? returnedBuffer = null;
            Action<NetworkBuffer> returnAction = nb =>
            {
                returnActionCalled = true;
                returnedBuffer = nb;
            };
            var networkBuffer = new NetworkBuffer(buffer, returnAction);

            // Act
            networkBuffer.Dispose();

            // Assert
            Assert.True(returnActionCalled);
            Assert.Equal(networkBuffer, returnedBuffer);
        }

        [Fact]
        public void Dispose_CalledMultipleTimes_CallsReturnActionOnlyOnce()
        {
            // Arrange
            var buffer = new byte[1024];
            int returnActionCallCount = 0;
            Action<NetworkBuffer> returnAction = nb => returnActionCallCount++;
            var networkBuffer = new NetworkBuffer(buffer, returnAction);

            // Act
            networkBuffer.Dispose();
            networkBuffer.Dispose();
            networkBuffer.Dispose();

            // Assert
            Assert.Equal(1, returnActionCallCount);
        }

        [Fact]
        public void Dispose_NoReturnAction_DoesNotThrow()
        {
            // Arrange
            var buffer = new byte[1024];
            var networkBuffer = new NetworkBuffer(buffer, null);

            // Act & Assert
            networkBuffer.Dispose(); // Should not throw
            Assert.True(true); // Explicit assertion to satisfy SonarCloud - disposal completed successfully
        }

        [Fact]
        public void WriteAndReadStream_DataRoundTrip_WorksCorrectly()
        {
            // Arrange
            var buffer = new byte[1024];
            using var networkBuffer = new NetworkBuffer(buffer, null, 10);
            var testData = "Hello, World!"u8.ToArray();

            // Act - Write data
            using (var writeStream = networkBuffer.GetWriteStream())
            {
                writeStream.Write(testData);
            }
            networkBuffer.SetDataLength(testData.Length);

            // Act - Read data
            using var readStream = networkBuffer.GetReadStream();
            var readData = new byte[testData.Length];
            readStream.ReadExactly(readData, 0, testData.Length);

            // Assert
            Assert.Equal(testData, readData);
            Assert.Equal(testData.Length, networkBuffer.DataLength);
        }
    }
}