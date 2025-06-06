#nullable enable
using FlinkDotNet.Core.Api.Windowing; // For Window

namespace FlinkDotNet.Core.Abstractions.Timers
{
    public enum TimerType { ProcessingTime, EventTime }

    /// <summary>
    /// Service responsible for managing and firing timers for keyed window operations.
    /// An instance is typically scoped per operator subtask.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TWindow">The type of the window.</typeparam>
    public interface ITimerService<TKey, TWindow> where TWindow : Window
    {
        void RegisterProcessingTimeTimer(TKey key, TWindow window, long timestamp);
        void DeleteProcessingTimeTimer(TKey key, TWindow window, long timestamp);
        void RegisterEventTimeTimer(TKey key, TWindow window, long timestamp);
        void DeleteEventTimeTimer(TKey key, TWindow window, long timestamp);

        /// <summary>
        /// Called by the system when processing time advances.
        /// </summary>
        /// <param name="newProcessingTime">The new current processing time.</param>
        void AdvanceProcessingTime(long newProcessingTime);

        /// <summary>
        /// Called by the system when the watermark for a specific key advances.
        /// </summary>
        /// <param name="key">The key whose watermark advanced.</param>
        /// <param name="newWatermark">The new watermark for the key.</param>
        void AdvanceKeyWatermark(TKey key, long newWatermark);

        /// <summary>
        /// Called by the system when the global watermark for the operator advances.
        /// Use this if watermarks are not tracked per key by the TimerService itself.
        /// </summary>
        /// <param name="newWatermark">The new global watermark.</param>
        void AdvanceGlobalWatermark(long newWatermark);

        // Methods for checkpointing timer state would also be here
        // Task SnapshotStateAsync(IStateSnapshotWriter writer, string timersStateName);
        // Task RestoreStateAsync(IStateSnapshotReader reader, string timersStateName);
    }
}
#nullable disable
