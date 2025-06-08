using FlinkDotNet.Core.Abstractions.Serializers;
using System; // Required for ArgumentNullException

namespace FlinkDotNet.Core.Abstractions.Models.State
{
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
}
