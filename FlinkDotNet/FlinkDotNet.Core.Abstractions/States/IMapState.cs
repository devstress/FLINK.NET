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
        /// Retrieves the value associated with the given key using GetValueForKey.
        /// </summary>
        /// <param name="key">The key to retrieve.</param>
        /// <returns>The value, or default(TV) if the key is not present.</returns>
        TV GetValueForKey(TK key);

        /// <summary>
        /// Compatibility alias matching older API that exposed <c>Get()</c>.
        /// </summary>
        TV Get(TK key);

        /// <summary>
        /// Associates the given value with the given key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        void Put(TK key, TV value);

        /// <summary>
        /// Copies all mappings from the specified dictionary to this map.
        /// </summary>
        /// <param name="map">The dictionary of key-value pairs to add.</param>
        void PutAll(IDictionary<TK, TV> map);

        /// <summary>
        /// Checks if the map contains a mapping for the specified key.
        /// </summary>
        /// <param name="key">The key to check.</param>
        /// <returns>True if the key exists, false otherwise.</returns>
        bool Contains(TK key);

        /// <summary>
        /// Removes the mapping for the specified key.
        /// </summary>
        /// <param name="key">The key to remove.</param>
        void Remove(TK key);

        /// <summary>
        /// Retrieves an enumerable of all keys in the map.
        /// </summary>
        IEnumerable<TK> Keys();

        /// <summary>
        /// Retrieves an enumerable of all values in the map.
        /// </summary>
        IEnumerable<TV> Values();

        /// <summary>
        /// Retrieves an enumerable of all key-value pairs (entries) in the map.
        /// </summary>
        IEnumerable<KeyValuePair<TK, TV>> Entries();

        /// <summary>
        /// Checks if the map contains no key-value mappings.
        /// </summary>
        /// <returns>True if the map is empty, false otherwise.</returns>
        bool IsEmpty();

        /// <summary>
        /// Removes all mappings from the map.
        /// </summary>
        void Clear(); // Common in Flink state interfaces
    }
}
