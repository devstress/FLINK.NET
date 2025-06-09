namespace FlinkDotNet.Core.Abstractions.Models.Checkpointing
{
    /// <summary>
    /// In-memory representation of a checkpoint barrier flowing through the data stream.
    /// This mirrors the corresponding gRPC message but keeps the core abstractions
    /// decoupled from the generated types.
    /// </summary>
    public class CheckpointBarrier
    {
        public long CheckpointId { get; set; }
        public long Timestamp { get; set; }
    }
}
