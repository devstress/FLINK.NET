using System.Collections.Generic; // Add if not present

namespace FlinkDotNet.Core.Abstractions.States
{
    /// <summary>
    /// Interface for a state that holds a single value.
    /// Similar to Flinks org.apache.flink.api.common.state.ValueState.
    /// </summary>
    /// <typeparam name="T">The type of the value in the state.</typeparam>
    public interface IValueState<T>
    {
        /// <summary>
        /// Retrieves the current value of the state.
        /// </summary>
        /// <returns>The current value, or default(T) if not set.</returns>
        T Value(); // Flink: T value() throws IOException;

        /// <summary>
        /// Updates the value of the state.
        /// </summary>
        /// <param name="value">The new value.</param>
        void Update(T value); // Flink: void update(T value) throws IOException;

        /// <summary>
        /// Deletes the value in the state, resetting it to default.
        /// </summary>
        void Clear(); // Common in Flink state interfaces

        /// <summary>
        /// Gets all keyed state entries for snapshotting.
        /// The keys are the actual keys used for partitioning state.
        /// </summary>
        /// <returns>A dictionary containing all key-value pairs for this state.</returns>
        IDictionary<object, T> GetSerializedStateEntries();

        /// <summary>
        /// Restores all keyed state entries from a provided collection.
        /// This should typically clear any existing state before populating.
        /// </summary>
        /// <param name="entries">The keyed data to restore.</param>
        void RestoreSerializedStateEntries(IDictionary<object, T> entries);
    }
}
