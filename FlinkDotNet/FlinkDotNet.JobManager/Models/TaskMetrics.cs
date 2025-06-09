namespace FlinkDotNet.JobManager.Models
{
    /// <summary>
    /// Basic metrics reported by a running task.
    /// Mirrored from the TaskManager project to avoid a project dependency.
    /// </summary>
    public class TaskMetrics
    {
        public string TaskId { get; set; } = string.Empty; // Format: JobVertexId_SubtaskIndex
        public long RecordsIn { get; set; }
        public long RecordsOut { get; set; }
    }
}
