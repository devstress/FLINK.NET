namespace FlinkDotNet.JobManager.Models
{
    public class DlqMessageDto
    {
        public string? MessageId { get; set; }
        public string? JobId { get; set; }
        public string? OriginalTopic { get; set; }
        public byte[]? Payload { get; set; } // Or string if payload is always text
        public string? ErrorReason { get; set; }
        public DateTime? FailedAt { get; set; }
        public Dictionary<string, string>? Headers { get; set; }
        // Content to be modified by the user
        public byte[]? NewPayload { get; set; }
        public DateTime? LastUpdated { get; set; } // Added for tracking updates
    }
}
