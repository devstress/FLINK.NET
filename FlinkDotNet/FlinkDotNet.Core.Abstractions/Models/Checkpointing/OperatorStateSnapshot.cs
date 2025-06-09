using System.Collections.Generic;

namespace FlinkDotNet.Core.Abstractions.Models.Checkpointing
{
    /// <summary>
    /// Represents the result of a single operator's snapshot during checkpointing.
    /// Contains the storage handle to the snapshot and optional metadata about the snapshot.
    /// </summary>
    public class OperatorStateSnapshot
    {
        public OperatorStateSnapshot(string stateHandle, long stateSize)
        {
            StateHandle = stateHandle;
            StateSize = stateSize;
        }

        public string StateHandle { get; set; }
        public long StateSize { get; set; }

        /// <summary>
        /// Optional metadata describing the snapshot.
        /// </summary>
        public Dictionary<string, string>? Metadata { get; set; }
    }
}
