#nullable enable
using System;
using System.Collections.Generic;
using FlinkDotNet.Core.Abstractions.Serializers; // For ITypeSerializer access if needed

namespace FlinkDotNet.JobManager.Models.JobGraph
{
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
