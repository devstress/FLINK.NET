namespace FlinkDotNet.WebUI.Models
{
    public class CheckpointInfoUIDto
    {
        public string? CheckpointId { get; set; } // Can be string if backend sends it as mock-cp-1 etc.
        public string? JobId { get; set; } // Though not in backend DTO, it's good for context if ever needed directly
        public DateTime Timestamp { get; set; }
        public string? Status { get; set; }
        public long DurationMs { get; set; } // Renamed from DurationMillis to match common C# convention
        public long SizeBytes { get; set; } // Renamed from SizeInBytes
    }
}
#nullable disable
