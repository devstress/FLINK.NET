using FlinkDotNet.Core.Abstractions.Windowing; // For Window

namespace FlinkDotNet.Core.Api.Windowing
{
    /// <summary>
    /// Context available to triggers, allowing them to register timers and access limited state.
    /// </summary>
    public interface ITriggerContext
    {
        long CurrentProcessingTime { get; }
        long CurrentWatermark { get; }
        void RegisterProcessingTimeTimer(long time);
        void DeleteProcessingTimeTimer(long time);
        void RegisterEventTimeTimer(long time);
        void DeleteEventTimeTimer(long time);
        // Add methods for trigger-specific state if needed, e.g.:
    }

    /// <summary>
    /// Abstract base class for all triggers. A trigger determines when a window
    /// should be fired (i.e., its window function evaluated and results emitted).
    /// </summary>
    /// <typeparam name="TElement">The type of elements in the stream.</typeparam>
    /// <typeparam name="TWindow">The type of <see cref="Window"/> to which this trigger can be applied.</typeparam>
    public abstract class Trigger<TElement, TWindow> where TWindow : Window
    {
        /// <summary>
        /// Called for every element that is assigned to a window.
        /// </summary>
        /// <param name="element">The element that was added.</param>
        /// <param name="timestamp">The timestamp of the element.</param>
        /// <param name="window">The window to which the element was assigned.</param>
        /// <param name="ctx">A context object that can be used to register timers and access state.</param>
        /// <returns>The action that should be taken on the window.</returns>
        public abstract TriggerResult OnElement(TElement element, long timestamp, TWindow window, ITriggerContext ctx);

        /// <summary>
        /// Called when a processing-time timer that was set using the trigger context fires.
        /// </summary>
        /// <param name="time">The timestamp of the fired timer.</param>
        /// <param name="window">The window for which the timer fired.</param>
        /// <param name="ctx">A context object that can be used to register timers and access state.</param>
        /// <returns>The action that should be taken on the window.</returns>
        public abstract TriggerResult OnProcessingTime(long time, TWindow window, ITriggerContext ctx);

        /// <summary>
        /// Called when an event-time timer that was set using the trigger context fires.
        /// </summary>
        /// <param name="time">The timestamp of the fired timer.</param>
        /// <param name="window">The window for which the timer fired.</param>
        /// <param name="ctx">A context object that can be used to register timers and access state.</param>
        /// <returns>The action that should be taken on the window.</returns>
        public abstract TriggerResult OnEventTime(long time, TWindow window, ITriggerContext ctx);

        /// <summary>
        /// Called when a window is merged into another window (e.g., in session windows).
        /// The state of the merged source window should be merged into the target window.
        /// </summary>
        /// <param name="window">The window into which others are merged.</param>
        /// <param name="mergeContext">Context allowing access to merged windows and state.</param>
        public virtual void OnMerge(TWindow window, IMergeTriggerContext<TWindow> mergeContext) { }
        // IMergeTriggerContext would provide access to state of merged windows. For now, it's a simple OnMerge.

        /// <summary>
        /// Performs any cleanup related to the given window, for example deleting per-window state
        /// that was created by the trigger.
        /// This is called when a window is purged.
        /// </summary>
        /// <param name="window">The window.</param>
        /// <param name="ctx">A context object that can be used to access state.</param>
        public abstract void Clear(TWindow window, ITriggerContext ctx);

        /// <summary>
        /// Determines whether this trigger can cause a window to merge with another.
        /// </summary>
        public virtual bool CanMerge => false; // Most triggers don't support merging. SessionWindowTrigger would override.
    }

    /// <summary>
    /// Context for merging windows, passed to Trigger.OnMerge.
    /// </summary>
    public interface IMergeTriggerContext<TWindow> : ITriggerContext where TWindow : Window
    {
        /// <summary>
        /// Merges the state of the source windows into the state of the target window.
        /// The trigger implementation is responsible for defining how state is merged.
        /// </summary>
        /// <param name="target">The window into which state is merged.</param>
        /// <param name="mergedWindows">The collection of windows being merged into the target.</param>
        void MergeState(TWindow target, ICollection<TWindow> mergedWindows);
    }
}
