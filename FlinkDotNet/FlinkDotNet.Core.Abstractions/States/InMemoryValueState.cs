#nullable enable // Enable nullable reference types for this file

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
        private bool _isSet;
        private readonly T _defaultValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryValueState{T}"/> class.
        /// </summary>
        /// <param name="defaultValue">The default value to return if the state has not been set.
        /// For reference types, this can be null. For value types, it will be their default (e.g., 0 for int)
        /// unless a different value is provided.</param>
        public InMemoryValueState(T defaultValue = default!)
        {
            _defaultValue = defaultValue;
            _isSet = false;
            _value = defaultValue;
        }

        /// <inheritdoc/>
        public T Value() // Returns T, which can be null if T is a reference type or Nullable<V>
        {
            return _isSet ? _value : _defaultValue;
        }

        /// <inheritdoc/>
        public void Update(T value) // Accepts T, which can be null if T is a reference type or Nullable<V>
        {
            _value = value;
            _isSet = true;
        }

        /// <inheritdoc/>
        public void Clear()
        {
            _value = _defaultValue;
            _isSet = false;
        }
    }
}
#nullable disable // Restore previous nullable context if needed
