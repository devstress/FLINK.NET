namespace FlinkDotNet.JobManager.Models
{
    public class OperatorDefinitionDto
    {
        /// <summary>
        /// Unique name for this operator step, used for defining connections.
        /// </summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Assembly-qualified class name of the user-defined function
        /// (e.g., ISourceFunction, IMapOperator, IFlatMapOperator, ISinkFunction, IReduceOperator, etc.).
        /// </summary>
        public string TypeName { get; set; } = string.Empty;

        /// <summary>
        /// Type of the operator, e.g., "Source", "Operator", "Sink".
        /// Used by JobManager to determine VertexType and apply appropriate logic.
        /// </summary>
        public string OperatorType { get; set; } = "Operator";

        public int Parallelism { get; set; } = 1;

        /// <summary>
        /// Names of upstream operators this operator consumes from.
        /// For a source, this list would be empty or null.
        /// </summary>
        public List<string>? Inputs { get; set; }

        /// <summary>
        /// Assembly-qualified name(s) of the input data type(s) for this operator.
        /// If multiple inputs, this list should correspond to the order in 'Inputs'.
        /// For the head of a chain, this is the external input type.
        /// For chained operators, this is the output type of the previous operator in the chain.
        /// </summary>
        public List<string>? InputDataTypes { get; set; } // Should ideally be singular for most ops, or map to named inputs.

        /// <summary>
        /// Assembly-qualified name of the output data type of this operator.
        /// For a chain, this is the output type of the *last* operator in the chain.
        /// For a sink, this might be null or the type it consumes.
        /// </summary>
        public string? OutputType { get; set; }


        // --- Enhancements ---

        /// <summary>
        /// Optional: Assembly-qualified type name of the ITypeSerializer for this operator's primary input.
        /// Applies to the head operator of a chain.
        /// </summary>
        public string? InputSerializerTypeName { get; set; }

        /// <summary>
        /// Optional: Assembly-qualified type name of the ITypeSerializer for this operator's primary output.
        /// Applies to the output of the last operator in a chain.
        /// </summary>
        public string? OutputSerializerTypeName { get; set; }

        /// <summary>
        /// Optional: Configuration for keying the output of this operator (or the last operator in its chain).
        /// </summary>
        public OutputKeyingConfigDto? OutputKeying { get; set; }

        /// <summary>
        /// Optional: A list of subsequent operator definitions to be chained to this operator.
        /// The 'Inputs' and 'Parallelism' of these chained DTOs are ignored.
        /// </summary>
        public List<OperatorDefinitionDto>? ChainedOperators { get; set; }

        /// <summary>
        /// Optional: Windowing configuration if this operator is a windowed computation.
        /// </summary>
        public WindowConfigDto? Windowing { get; set; }

        /// <summary>
        /// Generic properties for operator configuration, serialized as JSON string.
        /// Specific operators can deserialize this to their own config objects.
        /// Example: file path for a FileSource, or UDF constructor parameters.
        /// </summary>
        public string? OperatorPropertiesJson { get; set; }
    }
}
#nullable disable
