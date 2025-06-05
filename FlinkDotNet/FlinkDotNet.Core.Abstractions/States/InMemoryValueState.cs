#nullable enable // Enable nullable reference types for this file

using FlinkDotNet.Core.Abstractions.Serializers;

namespace FlinkDotNet.Core.Abstractions.States
{
    /// <summary>
    /// An in-memory implementation of <see cref="IValueState{T}"/>.
    /// This implementation is primarily for local testing and development.
    /// It is not designed for distributed fault tolerance.
    /// </summary>
    /// <typeparam name="T">The type of the value in the state.</typeparam>
    public class InMemoryValueState<T> : IValueState<T>
    {
        private T _value; // If T is a reference type, this can be null. If value type, cannot be null.
        // private readonly T _defaultValue; // DefaultValue is now part of the descriptor
        private readonly ITypeSerializer<T> _serializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryValueState{T}"/> class.
        /// </summary>
        /// <param name="initialValue">The initial value for the state.
        /// For reference types, this can be null. For value types, it will be their default (e.g., 0 for int)
        /// unless a different value is provided.</param>
        /// <param name="serializer">The serializer for the type T.</param>
        public InMemoryValueState(T initialValue, ITypeSerializer<T> serializer)
        {
            _serializer = serializer; // Store the serializer
            _value = initialValue; // Store initial value, could be default(T)
        }

        /// <inheritdoc/>
        public T Value() // Returns T, which can be null if T is a reference type or Nullable<V>
        {
            // The concept of a separate "defaultValue" at the state level is reduced
            // as the descriptor now holds it, and initialValue is passed at construction.
            return _value;
        }

        /// <inheritdoc/>
        public void Update(T value) // Accepts T, which can be null if T is a reference type or Nullable<V>
        {
            _value = value;
        }

        /// <inheritdoc/>
        public void Clear()
        {
            // Clearing should reset to the type's default, not a pre-configured default.
            // The descriptor's defaultValue is for when the state is first created.
            _value = default!; // This will be null for reference types, default for value types
        }
    }
}
#nullable disable // Restore previous nullable context if needed
