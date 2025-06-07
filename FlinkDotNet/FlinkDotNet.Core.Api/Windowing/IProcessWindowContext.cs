using FlinkDotNet.Core.Abstractions.Context; // For IRuntimeContext
using FlinkDotNet.Core.Abstractions.Models.State; // For state descriptors
using FlinkDotNet.Core.Abstractions.States;   // For state access (ValueStateDescriptor, etc.)

namespace FlinkDotNet.Core.Api.Windowing
{
    /// <summary>
    /// Base context for window operations, providing access to time and global runtime context.
    /// </summary>
    public abstract class WindowContext // Renamed from just Context to avoid ambiguity
    {
        /// <summary>
        /// Gets the IRuntimeContext of the task running this window operator.
        /// Provides access to global task information and non-windowed (operator) state.
        /// </summary>
        public abstract IRuntimeContext RuntimeContext { get; }

        /// <summary>
        /// Gets the current processing time.
        /// </summary>
        public abstract long CurrentProcessingTime { get; }

        /// <summary>
        /// Gets the current event time watermark for this operator instance.
        /// </summary>
        public abstract long CurrentWatermark { get; }
    }

    /// <summary>
    /// Context available to an <see cref="IProcessWindowFunction{TIn, TOut, TKey, TWindow}"/>.
    /// Provides information about the current window, key, time, and access to per-window state.
    /// </summary>
    public abstract class IProcessWindowContext<TKey, TWindow> : WindowContext
        where TWindow : Window
    {
        /// <summary>
        /// Gets the window that is being processed.
        /// </summary>
        public abstract TWindow Window { get; }

        /// <summary>
        /// Gets the key for the current window.
        /// (Note: Process method also receives key directly for convenience).
        /// </summary>
        public abstract TKey CurrentKey { get; }

        /// <summary>
        /// State accessor for per-window state. This state is scoped by the current key AND the current window.
        /// The state is automatically cleared when the window is purged by the system.
        /// </summary>
        public abstract IValueState<S> GetWindowState<S>(ValueStateDescriptor<S> stateDescriptor);

        /// <summary>
        /// State accessor for per-window list state.
        /// </summary>
        public abstract IListState<S> GetWindowListState<S>(ListStateDescriptor<S> stateDescriptor);

        // public abstract IMapState<SK, SV> GetWindowMapState<SK, SV>(MapStateDescriptor<SK, SV> stateDescriptor); // Future

        // Timer services (for advanced triggers and ProcessWindowFunction)
        // These interact with the event/processing time services of the operator.
        // public abstract void RegisterProcessingTimeTimer(long timestamp);
        // public abstract void DeleteProcessingTimeTimer(long timestamp);
        // public abstract void RegisterEventTimeTimer(long timestamp);
        // public abstract void DeleteEventTimeTimer(long timestamp);

        // Side output capability (advanced, for future)
        // public abstract void Output<X>(OutputTag<X> outputTag, X value);
    }
}
#nullable disable
