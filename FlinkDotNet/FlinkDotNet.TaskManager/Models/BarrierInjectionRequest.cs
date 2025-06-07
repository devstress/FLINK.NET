namespace FlinkDotNet.TaskManager.Models
{
    public record BarrierInjectionRequest(long CheckpointId, long CheckpointTimestamp);
}
