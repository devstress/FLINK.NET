using FlinkDotNet.Core.Abstractions.Serializers;
using System; // Required for ArgumentNullException

namespace FlinkDotNet.Core.Abstractions.Models.State
{
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
