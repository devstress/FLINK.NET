using FlinkDotNet.Core.Abstractions.Serializers;
using System; // Required for ArgumentNullException

namespace FlinkDotNet.Core.Abstractions.Models.State
{
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
}
