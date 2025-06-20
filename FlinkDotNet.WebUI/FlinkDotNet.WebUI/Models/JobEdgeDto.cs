namespace FlinkDotNet.WebUI.Models
{
    public class JobEdgeDto
    {
        public Guid Id { get; set; } // Added to match backend JobEdge
        public Guid SourceVertexId { get; set; }
        public Guid TargetVertexId { get; set; }
        public string? DataTypeName { get; set; }
        public string? SerializerTypeName { get; set; } // Added to match backend JobEdge
        public string? ShuffleMode { get; set; } // Added to match backend JobEdge (enum as string)
    }
}
