namespace FlinkDotNet.Core.Abstractions.Sources
{
    /// <summary>
    /// Base interface for sources that can operate in both bounded and unbounded modes.
    /// </summary>
    public interface IUnifiedSource<out T> : ISourceFunction<T>
    {
        /// <summary>
        /// Returns true if the source is bounded (batch) and will eventually finish.
        /// </summary>
        bool IsBounded { get; }
    }
}
