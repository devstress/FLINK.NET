using System.Collections.Generic;

namespace FlinkDotNet.Core.Abstractions.States
{
    /// <summary>
    /// Interface for a state that holds a list of values.
    /// Similar to Flinks org.apache.flink.api.common.state.ListState.
    /// </summary>
    /// <typeparam name="T">The type of the values in the list.</typeparam>
    public interface IListState<T>
    {
        /// <summary>
        /// Retrieves all elements currently in the list. (Renamed from Get to GetValues)
        /// </summary>
        /// <returns>An enumerable of elements, or an empty enumerable if the list is empty or not set.</returns>
        IEnumerable<T> GetValues();

        /// <summary>
        /// Compatibility alias matching older API that exposed <c>Get()</c>.
        /// </summary>
        IEnumerable<T> Get();

        /// <summary>
        /// Adds a single value to the list.
        /// </summary>
        /// <param name="value">The value to add.</param>
        void Add(T value);

        /// <summary>
        /// Adds all given values to the list.
        /// </summary>
        /// <param name="values">The collection of values to add.</param>
        void AddAll(IEnumerable<T> values);

        /// <summary>
        /// Replaces all current elements in the list with the given values.
        /// </summary>
        /// <param name="values">The new collection of values for the list.</param>
        void Update(IEnumerable<T> values);

        /// <summary>
        /// Deletes all elements from the list.
        /// </summary>
        void Clear(); // Common in Flink state interfaces
    }
}
