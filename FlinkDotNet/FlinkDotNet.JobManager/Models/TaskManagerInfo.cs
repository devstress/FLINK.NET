#nullable enable
using System;

namespace FlinkDotNet.JobManager.Models
{
    public class TaskManagerInfo
    {
        public string TaskManagerId { get; set; } = string.Empty;
        public string Address { get; set; } = string.Empty;
        public int Port { get; set; }
        public DateTime LastHeartbeat { get; set; }
        // Add more info like status, available slots etc.
    }
}
#nullable disable
