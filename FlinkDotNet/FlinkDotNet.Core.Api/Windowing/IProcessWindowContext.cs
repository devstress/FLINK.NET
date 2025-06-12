using FlinkDotNet.Core.Abstractions.Context; // For IRuntimeContext
using FlinkDotNet.Core.Abstractions.Models.State; // For state descriptors
using FlinkDotNet.Core.Abstractions.States;   // For state access (ValueStateDescriptor, etc.)
using FlinkDotNet.Core.Abstractions.Windowing; // For Window

namespace FlinkDotNet.Core.Api.Windowing
{
    /// <summary>
    /// Base context for window operations, providing access to time and global runtime context.
    /// </summary>
    public interface IWindowContext
    {
        /// <summary>
        /// Gets the IRuntimeContext of the task running this window operator.
        /// Provides access to global task information and non-windowed (operator) state.
        /// </summary>
        IRuntimeContext RuntimeContext { get; }

        /// <summary>
        /// Gets the current processing time.
        /// </summary>
        long CurrentProcessingTime { get; }

        /// <summary>
        /// Gets the current event time watermark for this operator instance.
        /// </summary>
        long CurrentWatermark { get; }
    }

    /// <summary>
    /// Context available to an <see cref="IProcessWindowFunction{TIn, TOut, TKey, TWindow}"/>.
    /// Provides information about the current window, key, time, and access to per-window state.
    /// </summary>
    public interface IProcessWindowContext<TKey, TWindow> : IWindowContext
        where TWindow : Window
    {
        /// <summary>
        /// Gets the window that is being processed.
        /// </summary>
        TWindow Window { get; }

        /// <summary>
        /// Gets the key for the current window.
        /// (Note: Process method also receives key directly for convenience).
        /// </summary>
        TKey CurrentKey { get; }

        /// <summary>
        /// State accessor for per-window state. This state is scoped by the current key AND the current window.
        /// The state is automatically cleared when the window is purged by the system.
        /// </summary>
        IValueState<S> GetWindowState<S>(ValueStateDescriptor<S> stateDescriptor);

        /// <summary>
        /// State accessor for per-window list state.
        /// </summary>
        IListState<S> GetWindowListState<S>(ListStateDescriptor<S> stateDescriptor);

        // public abstract IMapState<SK, SV> GetWindowMapState<SK, SV>(MapStateDescriptor<SK, SV> stateDescriptor); // Future

        // Timer services (for advanced triggers and ProcessWindowFunction)
        // These interact with the event/processing time services of the operator.

        // Side output capability (advanced, for future)
    }
}
