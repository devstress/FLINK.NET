#nullable enable
namespace FlinkDotNet.Core.Abstractions.Sources
{
    /// <summary>
    /// Context that is available to source functions, allowing them to emit records.
    /// </summary>
    /// <typeparam name="TOut">The type of the records produced by the source.</typeparam>
    public interface ISourceContext<TOut>
    {
        /// <summary>
        /// Emits a record from the source.
        /// </summary>
        /// <param name="record">The record to emit.</param>
        void Collect(TOut record);

        // In the future, this context can also provide access to:
        // - Getting the current processing time.
        // - Emitting watermarks.
        // - Accessing a lock for synchronized access to shared state/resources (if needed by the source).
    }
}
#nullable disable
