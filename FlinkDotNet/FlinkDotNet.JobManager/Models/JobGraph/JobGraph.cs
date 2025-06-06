#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    public class JobGraph
    {
        public Guid JobId { get; }
        public string JobName { get; }
        public List<JobVertex> Vertices { get; } = new();
        public List<JobEdge> Edges { get; } = new();

        /// <summary>
        /// Stores registered custom serializer type names.
        /// Key: Assembly-qualified name of the data type.
        /// Value: Assembly-qualified name of the ITypeSerializer<T> implementation for that data type.
        /// </summary>
        public Dictionary<string, string> SerializerTypeRegistrations { get; set; } = new();

        // TODO: Job-wide configurations (ExecutionConfig, CheckpointConfig, etc.)

        public JobGraph(string jobName)
        {
            JobId = Guid.NewGuid(); // Or could be passed in if known from submission
            JobName = jobName;
            // SerializerTypeRegistrations is initialized by its property initializer.
        }

        public void AddVertex(JobVertex vertex)
        {
            Vertices.Add(vertex);
        }

        public void AddEdge(JobVertex source, JobVertex target, string dataTypeName, string? serializerTypeName, ShuffleMode mode = ShuffleMode.Forward)
        {
            var edge = new JobEdge(source, target, dataTypeName, serializerTypeName)
            {
                ShuffleMode = mode
            };
            Edges.Add(edge);
        }

        public List<JobVertex> GetSourceVertices() => Vertices.Where(v => v.Type == VertexType.Source).ToList();
        public List<JobVertex> GetSinkVertices() => Vertices.Where(v => v.Type == VertexType.Sink).ToList();
    }
}
#nullable disable
