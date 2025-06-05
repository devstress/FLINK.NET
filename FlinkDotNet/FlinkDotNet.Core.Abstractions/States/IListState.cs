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
        /// Retrieves all elements currently in the list.
        /// </summary>
        /// <returns>An enumerable of elements, or an empty enumerable if the list is empty or not set.</returns>
        IEnumerable<T> Get(); // Flink: Iterable<T> get() throws Exception;

        /// <summary>
        /// Adds a single value to the list.
        /// </summary>
        /// <param name="value">The value to add.</param>
        void Add(T value); // Flink: void add(T value) throws Exception;

        /// <summary>
        /// Adds all given values to the list.
        /// </summary>
        /// <param name="values">The collection of values to add.</param>
        void AddAll(IEnumerable<T> values); // Flink: void addAll(List<T> values) throws Exception; (List in Java, IEnumerable in C# is more general)

        /// <summary>
        /// Replaces all current elements in the list with the given values.
        /// </summary>
        /// <param name="values">The new collection of values for the list.</param>
        void Update(IEnumerable<T> values); // Flink: void update(List<T> values) throws Exception;

        /// <summary>
        /// Deletes all elements from the list.
        /// </summary>
        void Clear(); // Common in Flink state interfaces
    }
}
