using FlinkDotNet.Core.Abstractions.Serializers; // For ITypeSerializer

using FlinkDotNet.Core.Abstractions.Windowing; // For Window, Trigger

namespace FlinkDotNet.Core.Api.Windowing
{
    /// <summary>
    /// Context passed to WindowAssigner methods, providing access to e.g. current processing time.
    /// </summary>
    public interface IWindowAssignerContext
    {
        /// <summary>
        /// Gets the current processing time.
        /// </summary>
        long CurrentProcessingTime { get; }
    }

    /// <summary>
    /// Default implementation of <see cref="IWindowAssignerContext"/>.
    /// </summary>
    public class DefaultWindowAssignerContext : IWindowAssignerContext
    {
        // In a real scenario, this might be provided by the TaskManager's time service.
        public long CurrentProcessingTime => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Could also hold a reference to the IRuntimeContext if assigners need more.
        // For now, keeping it minimal as per Flink's WindowAssigner.WindowAssignerContext.
    }

    /// <summary>
    /// A WindowAssigner assigns zero or more <see cref="Window"/>s to an element,
    /// based on the element, its timestamp, and context.
    /// </summary>
    /// <typeparam name="TElement">The type of elements to which windows are assigned.</typeparam>
    /// <typeparam name="TWindow">The type of <see cref="Window"/> that this assigner assigns.</typeparam>
    public interface IWindowAssigner<TElement, TWindow> where TWindow : Window
    {
        /// <summary>
        /// Given an element and its timestamp, returns the set of windows to which it should be assigned.
        /// </summary>
        /// <param name="element">The element to assign to windows.</param>
        /// <param name="timestamp">The timestamp of the element, used for time-based windows.</param>
        /// <param name="context">The <see cref="IWindowAssignerContext"/> in which the assigner operates.</param>
        /// <returns>A collection of windows.</returns>
        ICollection<TWindow> AssignWindows(TElement element, long timestamp, IWindowAssignerContext context);

        /// <summary>
        /// Returns the default <see cref="Trigger{TElement, TWindow}"/> for this.
        /// This trigger is used if no custom trigger is specified on the <see cref="WindowedStream{TElement, TKey, TWindow}"/>.
        /// </summary>
        /// <param name="environment">The stream execution environment.</param>
        Trigger<TElement, TWindow> GetDefaultTrigger(FlinkDotNet.Core.Api.StreamExecutionEnvironment environment);

        /// <summary>
        /// Returns an <see cref="ITypeSerializer{TWindow}"/> for serializing windows of type <c>TWindow</c>.
        /// This is crucial for checkpointing window state.
        /// </summary>
        ITypeSerializer<TWindow> GetWindowSerializer();

        /// <summary>
        /// Returns <c>true</c> if this assigner assigns windows based on event time, <c>false</c> otherwise (e.g., processing time).
        /// </summary>
        bool IsEventTime { get; }
    }
}
