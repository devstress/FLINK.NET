#nullable enable
using System;

namespace FlinkDotNet.JobManager.Models
{
    public class JobOverviewDto
    {
        public string JobId { get; set; } = string.Empty;
        public string JobName { get; set; } = string.Empty;
        public DateTime SubmissionTime { get; set; }
        public string Status { get; set; } = string.Empty;
        public TimeSpan? Duration { get; set; }
    }
}
#nullable disable
