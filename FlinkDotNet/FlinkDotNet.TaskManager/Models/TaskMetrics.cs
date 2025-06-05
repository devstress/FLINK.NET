#nullable enable
using System;

namespace FlinkDotNet.TaskManager.Models // Namespace adjusted for TaskManager project
{
    public class TaskMetrics
    {
        public string TaskId { get; set; } = string.Empty; // Format: JobVertexId_SubtaskIndex
        public long RecordsIn { get; set; }
        public long RecordsOut { get; set; }
        // Potentially other metrics like bytesIn, bytesOut, processingTimeMs, etc.
    }
}
#nullable disable
