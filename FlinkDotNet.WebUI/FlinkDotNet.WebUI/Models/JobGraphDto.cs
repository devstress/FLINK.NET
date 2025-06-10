namespace FlinkDotNet.WebUI.Models
{
    public class JobGraphDto
    {
        public Guid JobId { get; set; }
        public string? JobName { get; set; }
        public DateTime SubmissionTime { get; set; }
        public string? Status { get; set; }
        public List<JobVertexDto>? Vertices { get; set; }
        public List<JobEdgeDto>? Edges { get; set; }
    }
}
#nullable disable
