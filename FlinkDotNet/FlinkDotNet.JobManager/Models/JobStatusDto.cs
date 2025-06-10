namespace FlinkDotNet.JobManager.Models
{
    public class JobStatusDto
    {
        public string? JobId { get; set; }
        public string? Status { get; set; } // e.g., "RUNNING", "FAILED", "COMPLETED"
        public DateTime LastUpdated { get; set; }
        public string? ErrorMessage { get; set; }
        // Add other relevant status properties
    }
}
