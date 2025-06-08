namespace FlinkDotNet.Core.Abstractions.Windowing
{
    /// <summary>
    /// Base interface for all window types (e.g., TimeWindow, GlobalWindow).
    /// A Window is a logical grouping of elements from a stream.
    /// </summary>
    public interface IWindow
    {
        /// <summary>
        /// Gets the maximum timestamp that is included in this window.
        /// For time-based windows, this is typically the window end timestamp minus one.
        /// For other window types (like count or global), this might have different semantics
        /// or could represent a logical point in time.
        /// </summary>
        long MaxTimestamp();

        // It's important for Window objects to implement Equals and GetHashCode
        // correctly, as they are often used as keys in internal state (e.g., for per-window state).
        bool Equals(object? obj);
        int GetHashCode();
    }
}
