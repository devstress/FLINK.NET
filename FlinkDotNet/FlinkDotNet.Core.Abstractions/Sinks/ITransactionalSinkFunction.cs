using FlinkDotNet.Core.Abstractions.Context;

namespace FlinkDotNet.Core.Abstractions.Sinks
{
    /// <summary>
    /// Sink function with transaction hooks to allow exactly-once semantics.
    /// </summary>
    public interface ITransactionalSinkFunction<in T> : ISinkFunction<T>
    {
        /// <summary>
        /// Starts a new transaction and returns an identifier.
        /// </summary>
        string BeginTransaction();

        /// <summary>
        /// Called before a transaction is committed.
        /// </summary>
        void PreCommit(string transactionId);

        /// <summary>
        /// Commits the pending transaction.
        /// </summary>
        void Commit(string transactionId);

        /// <summary>
        /// Aborts the pending transaction.
        /// </summary>
        void Abort(string transactionId);
    }
}
