#nullable enable
using System;
using System.Collections.Generic;

namespace FlinkDotNet.WebUI.Models
{
    public class JobVertexDto
    {
        public Guid Id { get; set; }
        public string? Name { get; set; }
        public string? TypeName { get; set; } // Corresponds to VertexType in backend, but simplified as string for DTO
        public string? InputTypeName { get; set; }
        public string? OutputTypeName { get; set; }
        public int Parallelism { get; set; }
        public string? Status { get; set; } // Placeholder status
        public Dictionary<string, string>? Properties { get; set; } // Added to match backend JobVertex
        public List<JobEdgeDto>? InputEdges { get; set; } // Added to match backend JobVertex
        public List<JobEdgeDto>? OutputEdges { get; set; } // Added to match backend JobVertex
        public string? VertexType { get; set; } // To store the enum value as string if needed (e.g. "Source", "Operator", "Sink")

    }
}
#nullable disable
