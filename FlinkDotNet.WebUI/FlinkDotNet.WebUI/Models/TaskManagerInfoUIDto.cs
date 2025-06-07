using System;

namespace FlinkDotNet.WebUI.Models
{
    public class TaskManagerInfoUIDto
    {
        public string? TaskManagerId { get; set; }
        public string? Address { get; set; }
        public int Port { get; set; }
        public DateTime LastHeartbeat { get; set; }

        // Example of a calculated property for Status based on LastHeartbeat
        // This could be added if desired, but for now sticking to direct mapping.
        // public string Status => (DateTime.UtcNow - LastHeartbeat).TotalSeconds < 60 ? "Active" : "Unresponsive";
    }
}
#nullable disable
