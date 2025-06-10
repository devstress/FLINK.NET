namespace FlinkDotNet.JobManager.Models.JobGraph
{
    public class JobVertex
    {
        public Guid Id { get; }
        public string Name { get; } // User-defined name for this vertex
        public VertexType Type { get; } // Source, Operator, or Sink

        /// <summary>
        /// Definition of the operator, including its class name and configuration.
        /// </summary>
        public OperatorDefinition OperatorDefinition { get; }

        public int Parallelism { get; set; }

        // --- Fields from 'feature/comprehensive-flink-core-design' --- (and prior)
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

        // Properties are now part of OperatorDefinition.ConfigurationJson

        /// <summary>
        /// Stores keying information for output edges that require hash-based shuffling.
        /// Key is the JobEdge.Id of the output edge.
        /// </summary>
        public Dictionary<Guid, KeyingInfo> OutputEdgeKeying { get; } = new Dictionary<Guid, KeyingInfo>();

        /// <summary>
        /// List of operators chained to this vertex.
        /// </summary>
        public List<OperatorDefinition> ChainedOperators { get; } = new List<OperatorDefinition>();

        // --- Fields from 'main' (user's changes) ---
        public long AggregatedRecordsIn { get; set; } = 0; // This is runtime state, might not belong in core JobGraph model for submission
        public long AggregatedRecordsOut { get; set; } = 0; // This is runtime state, might not belong in core JobGraph model for submission

        // --- Connections ---
        // These lists store IDs of JobEdge instances. The JobGraph owns the JobEdge instances.
        private readonly List<Guid> _inputEdgeIds = new List<Guid>();
        private readonly List<Guid> _outputEdgeIds = new List<Guid>();

        // Read-only collections for external access to edge IDs
        public IReadOnlyList<Guid> InputEdgeIds => _inputEdgeIds.AsReadOnly();
        public IReadOnlyList<Guid> OutputEdgeIds => _outputEdgeIds.AsReadOnly();

        public JobVertex(
            string name,
            VertexType type,
            OperatorDefinition operatorDefinition,
            int parallelism = 1,
            string? inputTypeName = null,
            string? outputTypeName = null)
        {
            Id = Guid.NewGuid();
            Name = name;
            Type = type;
            OperatorDefinition = operatorDefinition;
            Parallelism = parallelism;
            InputTypeName = inputTypeName;
            OutputTypeName = outputTypeName;
        }

        // Methods to manage edge ID lists, typically called by JobGraph when adding/removing edges
        internal void AddInputEdgeId(Guid edgeId)
        {
            if (!_inputEdgeIds.Contains(edgeId)) _inputEdgeIds.Add(edgeId);
        }
        internal void RemoveInputEdgeId(Guid edgeId)
        {
            _inputEdgeIds.Remove(edgeId);
        }

        internal void AddOutputEdgeId(Guid edgeId)
        {
            if (!_outputEdgeIds.Contains(edgeId)) _outputEdgeIds.Add(edgeId);
        }
        internal void RemoveOutputEdgeId(Guid edgeId)
        {
            _outputEdgeIds.Remove(edgeId);
        }

        /// <summary>
        /// Converts this JobVertex model to its Protobuf representation.
        /// </summary>
        /// <returns>The Protobuf JobVertex message.</returns>
        public Proto.Internal.JobVertex ToProto()
        {
            var protoVertex = new Proto.Internal.JobVertex
            {
                Id = this.Id.ToString(),
                Name = this.Name,
                OperatorDefinition = this.OperatorDefinition.ToProto(), // Assumes OperatorDefinition has ToProto()
                Parallelism = this.Parallelism,
                VertexType = (Proto.Internal.VertexType)(int)this.Type // Simple enum cast
            };

            if (InputTypeName != null) protoVertex.InputTypeName = InputTypeName;
            if (OutputTypeName != null) protoVertex.OutputTypeName = OutputTypeName;
            if (InputSerializerTypeName != null) protoVertex.InputSerializerTypeName = InputSerializerTypeName;
            if (OutputSerializerTypeName != null) protoVertex.OutputSerializerTypeName = OutputSerializerTypeName;

            foreach (var edgeId in _inputEdgeIds)
            {
                protoVertex.InputEdgeIds.Add(edgeId.ToString());
            }

            foreach (var edgeId in _outputEdgeIds)
            {
                protoVertex.OutputEdgeIds.Add(edgeId.ToString());
            }

            foreach (var kvp in OutputEdgeKeying)
            {
                protoVertex.OutputEdgeKeying.Add(kvp.Key.ToString(), kvp.Value.ToProto());
            }

            // AggregatedRecordsIn and AggregatedRecordsOut are runtime metrics, not typically part of the static graph definition for submission.
            // If they need to be part of a status snapshot, they could be added to a different proto message or context.

            foreach (var chainedOperator in ChainedOperators)
            {
                protoVertex.ChainedOperators.Add(chainedOperator.ToProto());
            }

            return protoVertex;
        }

        /// <summary>
        /// Creates a JobVertex model from its Protobuf representation.
        /// </summary>
        /// <param name="protoVertex">The Protobuf JobVertex message.</param>
        /// <returns>A new JobVertex instance.</returns>
        public static JobVertex FromProto(Proto.Internal.JobVertex protoVertex)
        {
            var opDef = OperatorDefinition.FromProto(protoVertex.OperatorDefinition); // Assumes OperatorDefinition has FromProto

            var vertex = new JobVertex(
                protoVertex.Name,
                (VertexType)(int)protoVertex.VertexType, // Simple enum cast
                opDef,
                protoVertex.Parallelism,
                string.IsNullOrEmpty(protoVertex.InputTypeName) ? null : protoVertex.InputTypeName,
                string.IsNullOrEmpty(protoVertex.OutputTypeName) ? null : protoVertex.OutputTypeName
            )
            {
                // Id is Guid, proto is string. The JobVertex constructor creates a new Guid.
                // If protoVertex.Id needs to be preserved:
                // vertex.Id = Guid.Parse(protoVertex.Id); // This would require Id to have a setter or be handled differently.
                // For now, we let constructor assign a new ID, or assume proto ID matches if used consistently.
                InputSerializerTypeName = string.IsNullOrEmpty(protoVertex.InputSerializerTypeName) ? null : protoVertex.InputSerializerTypeName,
                OutputSerializerTypeName = string.IsNullOrEmpty(protoVertex.OutputSerializerTypeName) ? null : protoVertex.OutputSerializerTypeName
            };

            // InputEdgeIds and OutputEdgeIds are managed by JobGraph when edges are added.
            // They are not directly set here from protoVertex.InputEdgeIds / OutputEdgeIds
            // because those are derived from the Edges list in the JobGraph.
            // JobGraph.FromProto handles linking edges to vertices.

            foreach (var kvp in protoVertex.OutputEdgeKeying)
            {
                vertex.OutputEdgeKeying.Add(Guid.Parse(kvp.Key), KeyingInfo.FromProto(kvp.Value)); // Assumes KeyingInfo has FromProto
            }

            foreach (var protoChainedOperator in protoVertex.ChainedOperators)
            {
                vertex.ChainedOperators.Add(OperatorDefinition.FromProto(protoChainedOperator));
            }

            // Runtime metrics like AggregatedRecordsIn/Out are not set from the static graph definition
            return vertex;
        }
    }
}
#nullable disable
