using System;
using System.IO;

namespace FlinkDotNet.Core.Abstractions.Networking
{
    public interface INetworkBuffer : IDisposable
    {
        byte[] UnderlyingBuffer { get; }
        int DataOffset { get; }
        int DataLength { get; }
        int Capacity { get; }
        bool IsBarrierPayload { get; }
        long CheckpointId { get; }
        long CheckpointTimestamp { get; }

        void SetBarrierInfo(bool isBarrier, long checkpointId = 0, long checkpointTimestamp = 0);
        Memory<byte> GetMemory();
        ReadOnlyMemory<byte> GetReadOnlyMemory();
        Stream GetWriteStream();
        Stream GetReadStream();
        void SetDataLength(int length);
        void Reset();
    }
}
