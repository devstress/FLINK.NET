#nullable enable
using System;
using System.Collections.Concurrent; // Added for ConcurrentDictionary
using System.Collections.Generic;
using System.Linq;

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    public class JobGraph
    {
        public Guid JobId { get; }
        public string JobName { get; }
        public DateTime SubmissionTime { get; set; }
        public string Status { get; set; }
        public List<JobVertex> Vertices { get; } = new();
        public List<JobEdge> Edges { get; } = new();

        // Stores the latest cumulative metrics reported by each task instance (Key: "JobVertexId_SubtaskIndex")
        public ConcurrentDictionary<string, FlinkDotNet.TaskManager.Models.TaskMetrics> TaskInstanceMetrics { get; } = new();
        // Note: Using TaskManager.Models.TaskMetrics here. Ideally, JobManager would have its own identical DTO
        // to avoid direct dependency, but for this PoC, it's simpler.

        // TODO: Job-wide configurations (ExecutionConfig, CheckpointConfig, etc.)

        public JobGraph(string jobName)
        {
            JobId = Guid.NewGuid(); // Or could be passed in if known from submission
            JobName = jobName;
            SubmissionTime = DateTime.UtcNow;
            Status = "SUBMITTED";
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
