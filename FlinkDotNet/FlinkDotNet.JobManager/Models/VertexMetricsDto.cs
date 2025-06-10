namespace FlinkDotNet.JobManager.Models
{
    public class VertexMetricsDto
    {
        public string VertexId { get; set; } = string.Empty;
        public string VertexName { get; set; } = string.Empty;
        public long RecordsIn { get; set; }
        public long RecordsOut { get; set; }
    }
}
