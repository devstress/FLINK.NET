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
        T? Value();

        /// <summary>
        /// Updates the value of the state.
        /// </summary>
        /// <param name="value">The new value.</param>
        void Update(T value);

        /// <summary>
        /// Deletes the value in the state, resetting it to default.
        /// </summary>
        void Clear(); // Common in Flink state interfaces
    }
}
