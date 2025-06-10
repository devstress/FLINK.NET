namespace FlinkDotNet.WebUI.Models
{
    public class JobOverviewDto
    {
        public string? JobId { get; set; }
        public string? JobName { get; set; }
        public DateTime SubmissionTime { get; set; }
        public string? Status { get; set; }
        public TimeSpan? Duration { get; set; }
    }
}
#nullable disable
