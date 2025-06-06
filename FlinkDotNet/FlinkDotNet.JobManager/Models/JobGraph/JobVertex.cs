#nullable enable
using System;
using System.Collections.Generic;
// using FlinkDotNet.Core.Abstractions.Serializers; // Not directly needed here for DTO

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    // Assuming JobEdge, KeyingInfo, VertexType are defined (e.g., in JobGraph.cs or their own files)
    // If not, minimal stubs would be needed here.
    // For this subtask, we assume they are accessible from the JobGraph.cs definitions
    // as they are in the same namespace.

    public class JobVertex
    {
        public Guid Id { get; }
        public string Name { get; } // User-defined name for this vertex
        public VertexType Type { get; } // Source, Operator, or Sink
        
        /// <summary>
        /// Assembly-qualified class name of the user-defined function
        /// (e.g., ISourceFunction, IMapOperator, IFilterOperator, ISinkFunction, etc.).
        /// </summary>
        public string OperatorClassName { get; } // Renamed from 'TypeName' for clarity

        public int Parallelism { get; set; } = 1;

        // --- Fields from 'feature/comprehensive-flink-core-design' ---
        /// <summary>
        /// Assembly-qualified name of the primary input data type for this vertex's operator.
        /// Null for Source vertices.
        /// </summary>
        public string? InputTypeName { get; set; }

        /// <summary>
        /// Assembly-qualified name of the primary output data type of this vertex's operator.
        /// Null for Sink vertices.
        /// </summary>
        public string? OutputTypeName { get; set; }

        /// <summary>
        /// Optional: Assembly-qualified type name of the ITypeSerializer for this vertex's primary input.
        /// If null, a default will be used based on InputTypeName.
        /// </summary>
        public string? InputSerializerTypeName { get; set; }

        /// <summary>
        /// Optional: Assembly-qualified type name of the ITypeSerializer for this vertex's primary output.
        /// If null, a default will be used based on OutputTypeName.
        /// </summary>
        public string? OutputSerializerTypeName { get; set; }

        /// <summary>
        /// Generic properties for operator configuration, potentially from DTO's OperatorPropertiesJson.
        /// These would be used by TaskExecutor to configure the operator instance.
        /// </summary>
        public Dictionary<string, string> Properties { get; } = new();

        /// <summary>
        /// Stores keying information for output edges that require hash-based shuffling.
        /// Key is the JobEdge.Id of the output edge.
        /// </summary>
        public Dictionary<Guid, KeyingInfo> OutputEdgeKeying { get; } = new Dictionary<Guid, KeyingInfo>();
        
        // --- Fields from 'main' (user's changes) ---
        public long AggregatedRecordsIn { get; set; } = 0;
        public long AggregatedRecordsOut { get; set; } = 0;
        
        // --- Connections ---
        // These lists store IDs or lightweight references. The JobGraph manages the actual Edge instances.
        private readonly List<Guid> _inputEdgeIds = new List<Guid>();
        private readonly List<Guid> _outputEdgeIds = new List<Guid>();

        // Read-only collections for external access to edge IDs
        public IReadOnlyList<Guid> InputEdgeIds => _inputEdgeIds.AsReadOnly();
        public IReadOnlyList<Guid> OutputEdgeIds => _outputEdgeIds.AsReadOnly();


        public JobVertex(
            string name, 
            VertexType type, 
            string operatorClassName, 
            int parallelism = 1,
            string? inputTypeName = null, 
            string? outputTypeName = null 
            )
        {
            Id = Guid.NewGuid();
            Name = name;
            Type = type;
            OperatorClassName = operatorClassName;
            Parallelism = parallelism;
            InputTypeName = inputTypeName;
            OutputTypeName = outputTypeName;
        }

        // Methods to manage edge ID lists, typically called by JobGraph when adding edges
        internal void AddInputEdgeId(Guid edgeId) 
        {
            if (!_inputEdgeIds.Contains(edgeId)) _inputEdgeIds.Add(edgeId);
        }
        internal void AddOutputEdgeId(Guid edgeId)
        {
            if (!_outputEdgeIds.Contains(edgeId)) _outputEdgeIds.Add(edgeId);
        }

        // If direct JobEdge objects are needed by consumers of JobVertex,
        // they would typically be resolved via the JobGraph instance, e.g.,
        // public IEnumerable<JobEdge> GetInputEdges(JobGraph graph) => graph.Edges.Where(e => e.TargetVertexId == Id);
        // public IEnumerable<JobEdge> GetOutputEdges(JobGraph graph) => graph.Edges.Where(e => e.SourceVertexId == Id);
        // For simplicity as a DTO, direct JobEdge lists are avoided to prevent circular serialization issues
        // and keep the vertex focused on its own properties.
    }
}
#nullable disable
