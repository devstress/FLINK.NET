using System;

namespace FlinkDotNet.WebUI.Models
{
    public class TaskManagerInfoUIDto
    {
        public string? TaskManagerId { get; set; }
        public string? Address { get; set; }
        public int Port { get; set; }
        public DateTime LastHeartbeat { get; set; }
    }
}
