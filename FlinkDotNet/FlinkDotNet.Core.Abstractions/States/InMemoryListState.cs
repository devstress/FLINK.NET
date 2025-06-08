using FlinkDotNet.Core.Abstractions.Serializers;
using System.Collections.Generic;
using System.Linq; // Required for ToList() on IEnumerable for Update and AddAll

namespace FlinkDotNet.Core.Abstractions.States
{
    /// <summary>
    /// An in-memory implementation of <see cref="IListState{T}"/>.
    /// This implementation is primarily for local testing and development.
    /// It is not designed for distributed fault tolerance.
    /// </summary>
    /// <typeparam name="T">The type of the values in the list.</typeparam>
    public class InMemoryListState<T> : IListState<T>
    {
        private readonly List<T> _list;
        // private readonly ITypeSerializer<T> _elementSerializer; // S4487: Unread private field

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryListState{T}"/> class.
        /// </summary>
        /// <param name="elementSerializer">The serializer for the elements in the list.</param>
        public InMemoryListState(ITypeSerializer<T> elementSerializer)
        {
            // _elementSerializer = elementSerializer; // S4487: Unread private field
            _list = new List<T>();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryListState{T}"/> class
        /// with an initial list of elements and a serializer. (Primarily for testing/mocking setup)
        /// </summary>
        /// <param name="initialElements">The initial elements for the list state.</param>
        /// <param name="elementSerializer">The serializer for the elements in the list.</param>
        internal InMemoryListState(IEnumerable<T> initialElements, ITypeSerializer<T> elementSerializer)
        {
            // _elementSerializer = elementSerializer; // S4487: Unread private field
            _list = new List<T>(initialElements);
        }

        /// <inheritdoc/>
        public IEnumerable<T> Get()
        {
            // Return a copy or read-only view to prevent external modification of the internal list.
            // For simplicity of this in-memory version, we can return the list directly
            // if modification through the IEnumerable is not a concern for its typical usage pattern,
            // or ToList() for a shallow copy. Flink state backends often return iterables
            // that might throw ConcurrentModificationException if modified while iterating,
            // or they might return copies.
            // Returning a new list (ToList()) ensures that direct modification of the returned
            // IEnumerable doesnt affect the internal state directly, which is safer.
            return _list.ToList();
        }

        /// <inheritdoc/>
        public void Add(T value)
        {
            _list.Add(value);
        }

        /// <inheritdoc/>
        public void AddAll(IEnumerable<T> values)
        {
            if (values != null)
            {
                _list.AddRange(values);
            }
        }

        /// <inheritdoc/>
        public void Update(IEnumerable<T> values)
        {
            // Update replaces the entire list content
            _list.Clear();
            if (values != null)
            {
                _list.AddRange(values);
            }
        }

        /// <inheritdoc/>
        public void Clear()
        {
            _list.Clear();
        }
    }
}
