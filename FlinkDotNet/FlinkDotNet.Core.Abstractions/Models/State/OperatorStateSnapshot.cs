#nullable enable
using System.Collections.Generic;

namespace FlinkDotNet.Core.Abstractions.Models.State
{
    /// <summary>
    /// Represents the snapshot of an operator's state.
    /// </summary>
    public class OperatorStateSnapshot
    {
        /// <summary>
        /// A handle or path to where the primary state data is stored (e.g., file path, S3 key).
        /// </summary>
        public string? StateHandle { get; set; }

        /// <summary>
        /// Size of the state data in bytes.
        /// </summary>
        public long StateSize { get; set; }

        /// <summary>
        /// Optional: For sources, this can store their current offsets.
        /// Key: Source identifier (e.g., file path, topic_partition). Value: Offset (e.g., line number, Kafka offset).
        /// </summary>
        public Dictionary<string, string>? SourceOffsets { get; set; }

        /// <summary>
        /// Optional: Any additional metadata about the snapshot.
        /// </summary>
        public Dictionary<string, string>? Metadata { get; set; }

        public OperatorStateSnapshot() {} // Default constructor

        public OperatorStateSnapshot(string stateHandle, long stateSize)
        {
            StateHandle = stateHandle;
            StateSize = stateSize;
        }
    }
}
#nullable disable
