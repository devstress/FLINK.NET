using System.Collections.Generic;
using System.Threading.Tasks;

// Define placeholder request/reply types typically generated from .proto files for gRPC.
// For now, these are just C# classes to define the interface contract.
namespace FlinkDotNet.JobManager.InternalApiModels
{
    // ReportStateCompletion
    public class ReportStateCompletionRequest
    {
        public long CheckpointId { get; set; }
        public string? OperatorInstanceId { get; set; }
        public string? StateLocation { get; set; } // e.g., URI to blob storage
        public Dictionary<string, long>? InputOffsets { get; set; }
    }
    public class ReportStateCompletionReply
    {
        public bool Ack { get; set; } = true; // Simple acknowledgement
    }

    // RequestCheckpoint
    public class RequestCheckpointRequest
    {
        public long CheckpointId { get; set; }
        // Potentially other parameters like checkpoint type (full, incremental)
    }
    public class RequestCheckpointReply
    {
        public bool Accepted { get; set; } // Indicates if the TM will attempt the checkpoint
    }

    // RequestRecovery
    public class RequestRecoveryRequest
    {
        public string? JobId { get; set; }
        public long CheckpointId { get; set; }
        // Potentially details about which tasks/operators need recovery
    }
    public class RequestRecoveryReply
    {
        public bool RecoveryInitiated { get; set; }
    }

    // Heartbeat
    public class HeartbeatRequest
    {
        public string? JobId { get; set; }
        public string? OperatorInstanceId { get; set; } // Or TaskManagerId
        public string? HealthStatus { get; set; } // e.g., "HEALTHY", "DEGRADED"
        public Dictionary<string, double>? Metrics { get; set; } // e.g., { "cpuUsage": 0.75, "memoryUsageMB": 1024 }
    }
    public class HeartbeatReply
    {
        public bool Ack { get; set; } = true;
    }
}

namespace FlinkDotNet.JobManager.Interfaces
{
    using FlinkDotNet.JobManager.InternalApiModels;

    /// <summary>
    /// Defines the contract for the internal control plane APIs,
    /// typically used for communication between JobManager and TaskManagers (e.g., via gRPC).
    /// </summary>
    public interface IJobManagerInternalApi
    {
        /// <summary>
        /// TaskManager reports completion of its state persistence for a checkpoint.
        /// </summary>
        Task<ReportStateCompletionReply> ReportStateCompletion(ReportStateCompletionRequest request);

        /// <summary>
        /// JobManager requests a TaskManager to initiate a checkpoint.
        /// (Note: In Flink, barriers are injected into streams. This might be an alternative or complementary control path)
        /// </summary>
        Task<RequestCheckpointReply> RequestCheckpoint(RequestCheckpointRequest request);

        /// <summary>
        /// JobManager requests a TaskManager (or a new instance) to recover state for a job.
        /// </summary>
        Task<RequestRecoveryReply> RequestRecovery(RequestRecoveryRequest request);

        /// <summary>
        /// TaskManager sends a heartbeat to the JobManager.
        /// </summary>
        Task<HeartbeatReply> Heartbeat(HeartbeatRequest request);
    }
}
