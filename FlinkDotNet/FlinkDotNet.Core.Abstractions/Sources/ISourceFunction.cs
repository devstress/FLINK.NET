#nullable enable
using System.Threading;
using System.Threading.Tasks; // For Task, though Run is synchronous for now based on Flink's ISourceFunction

namespace FlinkDotNet.Core.Abstractions.Sources
{
    /// <summary>
    /// Interface for all Flink data sources.
    /// The main methods are <see cref="Run"/> and <see cref="Cancel"/>.
    /// Sources must be serializable if they are part of a JobGraph that gets serialized.
    /// </summary>
    /// <typeparam name="TOut">The type of the records produced by the source.</typeparam>
    public interface ISourceFunction<TOut> // Consider if this needs to be IAsyncSourceFunction later
    {
        /// <summary>
        /// Starts the source. Implementations can run in a loop to continuously read data.
        /// The source should not return from this method until it is canceled.
        /// Exceptions thrown from this method will cause the task to fail.
        /// </summary>
        /// <param name="ctx">The context to emit elements to and for accessing locking objects.</param>
        void Run(ISourceContext<TOut> ctx);

        /// <summary>
        /// Cancels the source. Most sources will implement this by breaking out of the
        /// main loop in the <see cref="Run"/> method.
        /// The method should block until the <see cref="Run"/> method has completely
        /// returned.
        /// </summary>
        void Cancel();

        // Potentially add Open(IRuntimeContext context) and Close() if sources need lifecycle
        // similar to rich functions, especially for checkpointed state.
        // For now, keeping it aligned with Flink's basic SourceFunction.
    }
}
#nullable disable
