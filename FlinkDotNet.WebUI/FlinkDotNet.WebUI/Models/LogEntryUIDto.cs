#nullable enable
using System;

namespace FlinkDotNet.WebUI.Models
{
    public class LogEntryUIDto
    {
        public DateTime Timestamp { get; set; }
        public string? Level { get; set; }
        public string? Message { get; set; }
    }
}
#nullable disable
