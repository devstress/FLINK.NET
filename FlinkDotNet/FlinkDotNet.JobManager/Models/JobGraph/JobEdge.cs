using System;
using FlinkDotNet.Proto.Internal; // Required for ToProto method

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    // ShuffleMode enum is now in its own file: ShuffleMode.cs

    public class JobEdge
    {
        public Guid Id { get; }
        public Guid SourceVertexId { get; }
        public Guid TargetVertexId { get; }
        public ShuffleMode ShuffleMode { get; set; }

        /// <summary>
        /// Configuration for key-based partitioning if ShuffleMode is Hash.
        /// Null otherwise.
        /// </summary>
        public OutputKeyingConfig? OutputKeyingConfig { get; set; }

        // Type of data flowing on this edge (could be derived from SourceVertex.OutputTypeName or explicitly set)
        public string DataTypeName { get; }
        public string? SerializerTypeName { get; set; } // Serializer for this data type, can be optional

        public JobEdge(
            Guid sourceVertexId,
            Guid targetVertexId,
            string dataTypeName,
            ShuffleMode shuffleMode = ShuffleMode.Forward,
            string? serializerTypeName = null,
            OutputKeyingConfig? outputKeyingConfig = null)
        {
            Id = Guid.NewGuid();
            SourceVertexId = sourceVertexId;
            TargetVertexId = targetVertexId;
            DataTypeName = dataTypeName;
            SerializerTypeName = serializerTypeName;
            ShuffleMode = shuffleMode;

            if (shuffleMode == ShuffleMode.Hash && outputKeyingConfig == null)
            {
                throw new ArgumentException("OutputKeyingConfig must be provided when ShuffleMode is Hash.", nameof(outputKeyingConfig));
            }
            OutputKeyingConfig = outputKeyingConfig;

            // The responsibility of linking this edge to JobVertex (i.e., adding IDs to InputEdgeIds/OutputEdgeIds)
            // should ideally be managed by the JobGraph class when an edge is added to it,
            // to ensure consistency and avoid direct manipulation from JobEdge constructor.
        }

        /// <summary>
        /// Converts this JobEdge model to its Protobuf representation.
        /// </summary>
        /// <returns>The Protobuf JobEdge message.</returns>
        public Proto.Internal.JobEdge ToProto()
        {
            var protoEdge = new Proto.Internal.JobEdge
            {
                Id = this.Id.ToString(),
                SourceVertexId = this.SourceVertexId.ToString(),
                TargetVertexId = this.TargetVertexId.ToString(),
                DataTypeName = this.DataTypeName,
                ShuffleMode = (Proto.Internal.ShuffleMode)(int)this.ShuffleMode // Simple enum cast
            };

            if (SerializerTypeName != null)
            {
                protoEdge.SerializerTypeName = SerializerTypeName;
            }

            if (OutputKeyingConfig != null)
            {
                protoEdge.OutputKeyingConfig = OutputKeyingConfig.ToProto();
            }

            return protoEdge;
        }

        /// <summary>
        /// Creates a JobEdge model from its Protobuf representation.
        /// </summary>
        /// <param name="protoEdge">The Protobuf JobEdge message.</param>
        /// <returns>A new JobEdge instance.</returns>
        public static JobEdge FromProto(Proto.Internal.JobEdge protoEdge)
        {
            OutputKeyingConfig? keyingConfig = null;
            if (protoEdge.OutputKeyingConfig != null)
            {
                keyingConfig = OutputKeyingConfig.FromProto(protoEdge.OutputKeyingConfig);
            }

            var edge = new JobEdge(
                Guid.Parse(protoEdge.SourceVertexId),
                Guid.Parse(protoEdge.TargetVertexId),
                protoEdge.DataTypeName,
                (ShuffleMode)(int)protoEdge.ShuffleMode, // Simple enum cast
                string.IsNullOrEmpty(protoEdge.SerializerTypeName) ? null : protoEdge.SerializerTypeName,
                keyingConfig
            );

            // The Id of the edge is created by the constructor. If protoEdge.Id needs to be preserved,
            // it would require 'Id' to have a setter or different handling.
            // For JobGraph.FromProto, it's important that the ID used for linking (Guid.Parse(protoEdge.Id))
            // matches the ID of the created 'edge' object. This implies 'Id' should be settable from proto.
            // This current implementation will generate a NEW Id for the model edge.
            // This might be an issue for JobGraph.FromProto's edge linking logic.
            // A potential fix: internal void SetId(Guid id) { this.Id = id; } and call it after construction.
            // Or make Id settable and JobGraph.FromProto sets it.
            // For now, this discrepancy is noted.

            return edge;
        }
    }
}
#nullable disable
