#nullable enable
using System;

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    public enum ShuffleMode
    {
        Forward,    // Pointwise connection
        Broadcast,
        Rescale,    // Round-robin or similar for rescaling parallelism
        Hash        // Keyed distribution
        // Add more as needed (e.g., RangePartition)
    }

    public class JobEdge
    {
        public Guid Id { get; }
        public JobVertex SourceVertex { get; }
        public JobVertex TargetVertex { get; }
        public ShuffleMode ShuffleMode { get; set; } = ShuffleMode.Forward;

        // Type of data flowing on this edge (could be derived from SourceVertex.OutputTypeName)
        public string DataTypeName { get; }
        public string? SerializerTypeName { get; } // Serializer for this data type

        public JobEdge(JobVertex sourceVertex, JobVertex targetVertex, string dataTypeName, string? serializerTypeName)
        {
            Id = Guid.NewGuid();
            SourceVertex = sourceVertex;
            TargetVertex = targetVertex;
            DataTypeName = dataTypeName;
            SerializerTypeName = serializerTypeName;

            // Automatically link this edge to the vertices
            sourceVertex.AddOutputEdge(this);
            targetVertex.AddInputEdge(this);
        }
    }
}
#nullable disable
