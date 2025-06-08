using System;

namespace FlinkDotNet.Core.Abstractions.Models.Checkpointing
{
    /// <summary>
    /// Represents a pointer or reference to a sink transaction associated with a checkpoint.
    /// Corresponds to an item in the "sinkTransactions" array in the Checkpoints JSON schema.
    /// </summary>
    public class SinkTransactionStatePointer
    {
        /// <summary>
        /// Identifier for the sink instance.
        /// </summary>
        public string? SinkInstanceId { get; set; }

        /// <summary>
        /// The unique transaction ID used with the external sink system.
        /// </summary>
        public Guid TransactionId { get; set; }
        // Potentially add status of this specific sink transaction if known at checkpoint time,
        // though the global SinkTransactions collection (Section 2.5 of design doc)
        // would be the primary source for transaction status.
    }
}
