using System.Collections.Generic;

namespace FlinkDotNet.Core.Abstractions.States
{
    /// <summary>
    /// Interface for a state that holds a map of key-value pairs.
    /// Similar to Flinks org.apache.flink.api.common.state.MapState.
    /// </summary>
    /// <typeparam name="TK">The type of the keys in the map.</typeparam>
    /// <typeparam name="TV">The type of the values in the map.</typeparam>
    public interface IMapState<TK, TV>
    {
        /// <summary>
        /// Retrieves the value associated with the given key.
        /// </summary>
        /// <param name="key">The key to retrieve.</param>
        /// <returns>The value, or default(TV) if the key is not present.</returns>
        TV Get(TK key); // Flink: UV get(UK key) throws Exception;

        /// <summary>
        /// Associates the given value with the given key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        void Put(TK key, TV value); // Flink: void put(UK key, UV value) throws Exception;

        /// <summary>
        /// Copies all mappings from the specified dictionary to this map.
        /// </summary>
        /// <param name="map">The dictionary of key-value pairs to add.</param>
        void PutAll(IDictionary<TK, TV> map); // Flink: void putAll(Map<UK, UV> map) throws Exception;

        /// <summary>
        /// Checks if the map contains a mapping for the specified key.
        /// </summary>
        /// <param name="key">The key to check.</param>
        /// <returns>True if the key exists, false otherwise.</returns>
        bool Contains(TK key); // Flink: boolean contains(UK key) throws Exception;

        /// <summary>
        /// Removes the mapping for the specified key.
        /// </summary>
        /// <param name="key">The key to remove.</param>
        void Remove(TK key); // Flink: void remove(UK key) throws Exception;

        /// <summary>
        /// Retrieves an enumerable of all keys in the map.
        /// </summary>
        IEnumerable<TK> Keys(); // Flink: Iterable<UK> keys() throws Exception;

        /// <summary>
        /// Retrieves an enumerable of all values in the map.
        /// </summary>
        IEnumerable<TV> Values(); // Flink: Iterable<UV> values() throws Exception;

        /// <summary>
        /// Retrieves an enumerable of all key-value pairs (entries) in the map.
        /// </summary>
        IEnumerable<KeyValuePair<TK, TV>> Entries(); // Flink: Iterable<Map.Entry<UK, UV>> entries() throws Exception;

        /// <summary>
        /// Checks if the map contains no key-value mappings.
        /// </summary>
        /// <returns>True if the map is empty, false otherwise.</returns>
        bool IsEmpty(); // Flink: boolean isEmpty() throws Exception;

        /// <summary>
        /// Removes all mappings from the map.
        /// </summary>
        void Clear(); // Common in Flink state interfaces
    }
}
