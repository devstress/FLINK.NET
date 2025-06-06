#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
// Assuming FlinkDotNet.TaskManager.Models.TaskMetrics is accessible.
// If TaskMetrics is in a different namespace, this using might need to be adjusted,
// or TaskMetrics might need to be defined/mirrored in JobManager.Models.
using FlinkDotNet.TaskManager.Models; // Added based on assumption

// Assuming JobVertex, JobEdge, VertexType are defined in this namespace or accessible.
// If not, they would need to be defined for compilation. For this subtask, we focus on JobGraph.
// Minimal stubs if they are not found by the tool from other files:
// public enum VertexType { Source, Operator, Sink }
// public class JobVertex { public Guid Id { get; } public VertexType Type { get; set; } /* ... other properties */ }
// public class JobEdge { /* ... properties ... */ }
// public enum ShuffleMode { Forward, Broadcast, Hash, RoundRobin } // Example, if used by JobEdge

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    public enum VertexType // Ensure VertexType is defined if not elsewhere
    {
        Source,
        Operator,
        Sink
    }

    public class JobVertex // Ensure JobVertex is defined if not elsewhere
    {
        public Guid Id { get; internal set; } // Settable by JobGraph/Builder
        public string Name { get; set; } = "";
        public VertexType Type { get; set; }
        // Add other necessary properties that JobGraph might interact with or JobGraphBuilder sets
        public string OperatorTypeName { get; set; } = ""; // Fully qualified name of the operator/function
        public int Parallelism { get; set; } = 1;
        public string? InputTypeName { get; set; }
        public string? OutputTypeName { get; set; }
        public string? InputSerializerTypeName { get; set; }
        public string? OutputSerializerTypeName { get; set; }
        public Dictionary<Guid, KeyingInfo> OutputEdgeKeying { get; } = new(); // Added from previous step for keying
        // public List<JobEdge> InputEdges { get; } = new(); // Managed by JobGraph
        // public List<JobEdge> OutputEdges { get; } = new(); // Managed by JobGraph
        public Dictionary<string, string> Properties { get; set; } = new();


        // Constructor for JobVertex
        public JobVertex(string name, VertexType type, string operatorTypeName, int parallelism = 1)
        {
            Id = Guid.NewGuid();
            Name = name;
            Type = type;
            OperatorTypeName = operatorTypeName;
            Parallelism = parallelism;
        }
    }

    public class JobEdge // Ensure JobEdge is defined if not elsewhere
    {
        public Guid Id { get; }
        public Guid SourceVertexId { get; }
        public Guid TargetVertexId { get; }
        public string DataTypeName { get; } // Assembly-qualified name of the data type on this edge
        public string? SerializerTypeName { get; set; } // Optional: Specific serializer for this edge
        public ShuffleMode ShuffleMode { get; set; } = ShuffleMode.Forward;

        // References to actual vertices - careful with direct circular dependencies if not handled well
        // public JobVertex SourceVertex { get; } 
        // public JobVertex TargetVertex { get; }

        public JobEdge(JobVertex source, JobVertex target, string dataTypeName, string? serializerTypeName = null)
        {
            Id = Guid.NewGuid();
            SourceVertexId = source.Id;
            TargetVertexId = target.Id;
            DataTypeName = dataTypeName;
            SerializerTypeName = serializerTypeName;
            // SourceVertex = source; // Potentially problematic for simple DTOs if used for serialization
            // TargetVertex = target;
        }
    }
    
    public class KeyingInfo // Copied from JobVertex.cs if it was defined there, for completeness
    {
        public string? SerializedKeySelector { get; set; } 
        public string? KeyTypeName { get; set; }
    }


    public enum ShuffleMode // Ensure ShuffleMode is defined
    {
        Forward,
        Broadcast,
        Hash,
        RoundRobin
    }


    public class JobGraph
    {
        public Guid JobId { get; }
        public string JobName { get; }
        public List<JobVertex> Vertices { get; } = new();
        public List<JobEdge> Edges { get; } = new();

        // --- Fields from 'main' (user's changes) ---
        public DateTime SubmissionTime { get; set; }
        public string Status { get; set; } // Examples: SUBMITTED, RUNNING, FAILED, COMPLETED
        
        public ConcurrentDictionary<string, FlinkDotNet.TaskManager.Models.TaskMetrics> TaskInstanceMetrics { get; } = new();

        // --- Fields from 'feature/comprehensive-flink-core-design' (Jules's conceptual changes) ---
        public Dictionary<string, string> SerializerTypeRegistrations { get; set; } = new();


        public JobGraph(string jobName)
        {
            JobId = Guid.NewGuid();
            JobName = jobName;
            SubmissionTime = DateTime.UtcNow;
            Status = "SUBMITTED"; 
        }

        public void AddVertex(JobVertex vertex)
        {
            Vertices.Add(vertex);
        }

        public void AddEdge(JobEdge edge) 
        {
            Edges.Add(edge);
        }
        
        public List<JobVertex> GetSourceVertices() => Vertices.Where(v => v.Type == VertexType.Source).ToList();
        public List<JobVertex> GetSinkVertices() => Vertices.Where(v => v.Type == VertexType.Sink).ToList();
    }
}

namespace FlinkDotNet.TaskManager.Models // Placeholder if the actual namespace/file isn't present
{
    public class TaskMetrics // Dummy placeholder
    {
        public long RecordsIn { get; set; }
        public long RecordsOut { get; set; }
    }
}
#nullable disable
