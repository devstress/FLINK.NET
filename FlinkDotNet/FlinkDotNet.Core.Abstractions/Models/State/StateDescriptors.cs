#nullable enable // Enable nullable reference types for this file

using FlinkDotNet.Core.Abstractions.Serializers;
using System; // Required for ArgumentNullException

namespace FlinkDotNet.Core.Abstractions.Models.State
{
    /// <summary>
    /// Base class for state descriptors. Contains the name of the state.
    /// </summary>
    public abstract class StateDescriptor
    {
        /// <summary>
        /// Gets the name of the state.
        /// </summary>
        public string Name { get; }

        protected StateDescriptor(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new System.ArgumentException("State name cannot be null or whitespace.", nameof(name));
            }
            Name = name;
        }

        // Future: Add TypeSerializer<T> properties here or in derived classes.
        // public abstract System.Type StateType { get; }
    }

    /// <summary>
    /// Descriptor for <see cref="FlinkDotNet.Core.Abstractions.States.IValueState{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the value in the state.</typeparam>
    public class ValueStateDescriptor<T> : StateDescriptor
    {
        /// <summary>
        /// Gets the default value for the state.
        /// This is returned by <see cref="FlinkDotNet.Core.Abstractions.States.IValueState{T}.Value()"/> if no value was set.
        /// </summary>
        public T? DefaultValue { get; }

        public ITypeSerializer<T> Serializer { get; }

        public ValueStateDescriptor(string name, ITypeSerializer<T> serializer, T? defaultValue = default)
            : base(name)
        {
            Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            DefaultValue = defaultValue;
        }
    }

    /// <summary>
    /// Descriptor for <see cref="FlinkDotNet.Core.Abstractions.States.IListState{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the list state.</typeparam>
    public class ListStateDescriptor<T> : StateDescriptor
    {
        public ITypeSerializer<T> ElementSerializer { get; }

        public ListStateDescriptor(string name, ITypeSerializer<T> elementSerializer)
            : base(name)
        {
            ElementSerializer = elementSerializer ?? throw new ArgumentNullException(nameof(elementSerializer));
        }
    }

    /// <summary>
    /// Descriptor for <see cref="FlinkDotNet.Core.Abstractions.States.IMapState{TK, TV}"/>.
    /// </summary>
    /// <typeparam name="TK">The type of the keys in the map state.</typeparam>
    /// <typeparam name="TV">The type of the values in the map state.</typeparam>
    public class MapStateDescriptor<TK, TV> : StateDescriptor
    {
        public ITypeSerializer<TK> KeySerializer { get; }
        public ITypeSerializer<TV> ValueSerializer { get; }

        public MapStateDescriptor(string name, ITypeSerializer<TK> keySerializer, ITypeSerializer<TV> valueSerializer)
            : base(name)
        {
            KeySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
            ValueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
        }
    }
}
#nullable disable // Restore previous nullable context if needed outside this file
