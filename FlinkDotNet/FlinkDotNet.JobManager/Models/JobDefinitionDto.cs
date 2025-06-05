using System.Collections.Generic;

namespace FlinkDotNet.JobManager.Models
{
    public class JobDefinitionDto
    {
        public string? JobName { get; set; }
        public string? JobConfiguration { get; set; } // Could be JSON/YAML string
        // Add other relevant properties for a job definition
        public Dictionary<string, string>? Tags { get; set; }
    }
}
