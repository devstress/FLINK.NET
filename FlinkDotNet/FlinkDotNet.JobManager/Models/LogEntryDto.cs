namespace FlinkDotNet.JobManager.Models
{
    public class LogEntryDto
    {
        public DateTime Timestamp { get; set; }
        public string Level { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }
}
