#nullable enable // Enable nullable reference types for this file

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

        // public TypeSerializer<T> Serializer { get; } // Example for later

        public ValueStateDescriptor(string name, T? defaultValue = default)
            : base(name)
        {
            DefaultValue = defaultValue;
            // Serializer = serializer ?? throw new System.ArgumentNullException(nameof(serializer));
        }
    }

    /// <summary>
    /// Descriptor for <see cref="FlinkDotNet.Core.Abstractions.States.IListState{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the list state.</typeparam>
    public class ListStateDescriptor<T> : StateDescriptor
    {
        // public TypeSerializer<T> ElementSerializer { get; } // Example for later

        public ListStateDescriptor(string name /*, TypeSerializer<T> elementSerializer */)
            : base(name)
        {
            // ElementSerializer = elementSerializer ?? throw new System.ArgumentNullException(nameof(elementSerializer));
        }
    }

    /// <summary>
    /// Descriptor for <see cref="FlinkDotNet.Core.Abstractions.States.IMapState{TK, TV}"/>.
    /// </summary>
    /// <typeparam name="TK">The type of the keys in the map state.</typeparam>
    /// <typeparam name="TV">The type of the values in the map state.</typeparam>
    public class MapStateDescriptor<TK, TV> : StateDescriptor
    {
        // public TypeSerializer<TK> KeySerializer { get; } // Example for later
        // public TypeSerializer<TV> ValueSerializer { get; } // Example for later

        public MapStateDescriptor(string name /*, TypeSerializer<TK> keySerializer, TypeSerializer<TV> valueSerializer */)
            : base(name)
        {
            // KeySerializer = keySerializer ?? throw new System.ArgumentNullException(nameof(keySerializer));
            // ValueSerializer = valueSerializer ?? throw new System.ArgumentNullException(nameof(valueSerializer));
        }
    }
}
#nullable disable // Restore previous nullable context if needed outside this file
