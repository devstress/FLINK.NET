using System;
using System.Collections.Generic;

namespace FlinkDotNet.JobManager.Models
{
    public class CheckpointInfoDto
    {
        public string? CheckpointId { get; set; }
        public DateTime Timestamp { get; set; }
        public string? Status { get; set; } // e.g., "COMPLETED", "IN_PROGRESS"
        public long DurationMs { get; set; }
        public long SizeBytes { get; set; }
    }
}
