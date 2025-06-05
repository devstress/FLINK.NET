#nullable enable
using FlinkDotNet.Core.Abstractions.Serializers;
using System.Collections.Generic;
using System.Linq; // Required for Keys, Values, Entries returning copies

namespace FlinkDotNet.Core.Abstractions.States
{
    /// <summary>
    /// An in-memory implementation of <see cref="IMapState{TK, TV}"/>.
    /// This implementation is primarily for local testing and development.
    /// It is not designed for distributed fault tolerance.
    /// </summary>
    /// <typeparam name="TK">The type of the keys in the map.</typeparam>
    /// <typeparam name="TV">The type of the values in the map.</typeparam>
    public class InMemoryMapState<TK, TV> : IMapState<TK, TV> where TK : notnull
    {
        private readonly Dictionary<TK, TV> _dictionary;
        private readonly ITypeSerializer<TK> _keySerializer;
        private readonly ITypeSerializer<TV> _valueSerializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryMapState{TK, TV}"/> class.
        /// </summary>
        /// <param name="keySerializer">The serializer for the keys.</param>
        /// <param name="valueSerializer">The serializer for the values.</param>
        public InMemoryMapState(ITypeSerializer<TK> keySerializer, ITypeSerializer<TV> valueSerializer)
        {
            _keySerializer = keySerializer;
            _valueSerializer = valueSerializer;
            _dictionary = new Dictionary<TK, TV>();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryMapState{TK, TV}"/> class
        /// with an initial set of entries and serializers. (Primarily for testing/mocking setup)
        /// </summary>
        /// <param name="initialEntries">The initial entries for the map state.</param>
        /// <param name="keySerializer">The serializer for the keys.</param>
        /// <param name="valueSerializer">The serializer for the values.</param>
        internal InMemoryMapState(IDictionary<TK, TV> initialEntries, ITypeSerializer<TK> keySerializer, ITypeSerializer<TV> valueSerializer)
        {
            _keySerializer = keySerializer;
            _valueSerializer = valueSerializer;
            _dictionary = new Dictionary<TK, TV>(initialEntries);
        }

        /// <inheritdoc/>
        public TV Get(TK key)
        {
            // If TV is a non-nullable reference type, IMapState<TK, TV>.Get implies it should not return null.
            // Standard Dictionary<TK,TV>.TryGetValue sets `value` to default(TV) (which is null for ref types) if key not found.
            // To correctly implement non-nullable TV return type, we should ideally throw if not found,
            // or the interface should be TV? Get(TK key).
            // Given the current interface, returning default(TV) (which can be null for ref types) is a common pattern.
            // The null-forgiving operator tells the compiler we accept this.
            _dictionary.TryGetValue(key, out TV? value); // Use TV? for the out parameter
            return value!; // If key not found, value is default(TV?), which is null. value! asserts it's not null if TV is non-nullable ref type.
                           // This will lead to a runtime NullReferenceException if TV is non-nullable string and key not found.
                           // A better approach for non-nullable TV would be to throw KeyNotFoundException.
                           // However, to just silence the warning and return default(TV) (null for ref types):
        }

        /// <inheritdoc/>
        public void Put(TK key, TV value)
        {
            _dictionary[key] = value;
        }

        /// <inheritdoc/>
        public void PutAll(IDictionary<TK, TV> map)
        {
            if (map != null)
            {
                foreach (var entry in map)
                {
                    _dictionary[entry.Key] = entry.Value;
                }
            }
        }

        /// <inheritdoc/>
        public bool Contains(TK key)
        {
            return _dictionary.ContainsKey(key);
        }

        /// <inheritdoc/>
        public void Remove(TK key)
        {
            _dictionary.Remove(key);
        }

        /// <inheritdoc/>
        public IEnumerable<TK> Keys()
        {
            return _dictionary.Keys.ToList();
        }

        /// <inheritdoc/>
        public IEnumerable<TV> Values()
        {
            return _dictionary.Values.ToList();
        }

        /// <inheritdoc/>
        public IEnumerable<KeyValuePair<TK, TV>> Entries()
        {
            return _dictionary.ToList();
        }

        /// <inheritdoc/>
        public bool IsEmpty()
        {
            return _dictionary.Count == 0;
        }

        /// <inheritdoc/>
        public void Clear()
        {
            _dictionary.Clear();
        }
    }
}
#nullable disable
