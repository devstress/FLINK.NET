#nullable enable
using System.Collections.Generic; // For List

namespace FlinkDotNet.JobManager.Models
{
    // Keep existing properties if any (e.g., JobName)
    public class JobDefinitionDto
    {
        public string JobName { get; set; } = "UnnamedJob";

        // New properties for a simple job structure
        public SourceDefinitionDto? Source { get; set; }
        public List<OperatorDefinitionDto>? Operators { get; set; } // For a chain of operators, though we'll start with one
        public SinkDefinitionDto? Sink { get; set; }

        // Later, this will evolve to a graph with connections, parallelism etc.
    }

    public class SourceDefinitionDto
    {
        public string Name { get; set; } = "DefaultSource";
        public string TypeName { get; set; } = ""; // Fully qualified name of the ISourceFunction implementation
        public string OutputTypeName { get; set; } = ""; // Fully qualified name of the output type (e.g., "System.String")
        public string SerializerTypeName { get; set; } = ""; // Fully qualified name of ITypeSerializer for OutputType
        public Dictionary<string, string> Properties { get; set; } = new(); // e.g., "filePath" for FileSource
    }

    public class OperatorDefinitionDto // Simplified for now
    {
        public string Name { get; set; } = "DefaultOperator";
        public string TypeName { get; set; } = ""; // Fully qualified name of the operator (e.g., IMapOperator)
        // Input/Output types might be inferred or explicitly set later
        public Dictionary<string, string> Properties { get; set; } = new();
    }

    public class SinkDefinitionDto
    {
        public string Name { get; set; } = "DefaultSink";
        public string TypeName { get; set; } = ""; // Fully qualified name of the ISinkFunction implementation
        public string InputTypeName { get; set; } = ""; // Fully qualified name of the input type
        public Dictionary<string, string> Properties { get; set; } = new();
    }
}
#nullable disable
