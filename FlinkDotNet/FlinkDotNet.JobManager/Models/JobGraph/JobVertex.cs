#nullable enable
using System;
using System.Collections.Generic; // For List, Dictionary
using FlinkDotNet.Core.Abstractions.Serializers; // For ITypeSerializer access if needed

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    // Assume VertexType, JobEdge are defined elsewhere or in this file if not seen
    // public enum VertexType { Source, Operator, Sink } // Placeholder if not defined // Already defined below

    public class KeyingInfo
    {
        public string? SerializedKeySelector { get; set; } // Placeholder for how KeySelector is passed
        public string? KeyTypeName { get; set; }
        // Potentially add KeySerializerTypeName if keys need special serialization
    }

    public enum VertexType
    {
        Source,
        Operator,
        Sink
    }

    public class JobVertex
    {
        public Guid Id { get; }
        public string Name { get; }
        public VertexType Type { get; }
        public string TypeName { get; } // Fully qualified name of the ISourceFunction, IOperator, or ISinkFunction
        public int Parallelism { get; set; } = 1;

        // Type information for inputs/outputs - crucial for connecting edges and serialization
        public string? InputTypeName { get; set; }  // For Operators and Sinks
        public string? OutputTypeName { get; set; } // For Sources and Operators

        // Serializer information (fully qualified type names)
        public string? InputSerializerTypeName { get; set; }
        public string? OutputSerializerTypeName { get; set; }


        public Dictionary<string, string> Properties { get; } = new();

        // Connections
        public List<JobEdge> InputEdges { get; } = new();
        public List<JobEdge> OutputEdges { get; } = new();

        /// <summary>
        /// Stores keying information for output edges that require hash-based shuffling.
        /// Key is the JobEdge.Id of the output edge.
        /// </summary>
        public Dictionary<Guid, KeyingInfo> OutputEdgeKeying { get; } = new Dictionary<Guid, KeyingInfo>();

        public JobVertex(string name, VertexType type, string typeName, int parallelism = 1)
        {
            Id = Guid.NewGuid();
            Name = name;
            Type = type;
            TypeName = typeName;
            Parallelism = parallelism;
        }

        public void AddInputEdge(JobEdge edge) => InputEdges.Add(edge);
        public void AddOutputEdge(JobEdge edge) => OutputEdges.Add(edge);

        // TODO: Methods to configure state backend per operator, resource requirements, etc.
    }
}
#nullable disable
