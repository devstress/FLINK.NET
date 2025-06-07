using FlinkDotNet.Core.Abstractions.Context; // For IRuntimeContext if needed later

namespace FlinkDotNet.Core.Abstractions.Sinks
{
    /// <summary>
    /// Context that is available to sink functions.
    /// </summary>
    public interface ISinkContext
    {
        /// <summary>
        /// Gets the current processing time in milliseconds.
        /// </summary>
        long CurrentProcessingTimeMillis();

        // In the future, this context can also provide access to:
        // - The IRuntimeContext of the surrounding operator.
        // - Information about the subtask index.
        // - Watermarks (if the sink needs to react to them).
    }
}
#nullable disable
