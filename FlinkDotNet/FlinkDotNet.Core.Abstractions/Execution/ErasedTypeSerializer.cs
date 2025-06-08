using System;
using System.Linq; // For FirstOrDefault
using System.Reflection;
using FlinkDotNet.Core.Abstractions.Serializers;

namespace FlinkDotNet.Core.Abstractions.Execution
{
    internal sealed class ErasedTypeSerializer : ITypeSerializer<object>
    {
        private readonly object _specificSerializer;
        private readonly MethodInfo _serializeMethod;
        private readonly MethodInfo _deserializeMethod;
        private readonly Type _specificSerializerTypeArgument;

        public ErasedTypeSerializer(object specificSerializerInstance)
        {
            _specificSerializer = specificSerializerInstance ?? throw new ArgumentNullException(nameof(specificSerializerInstance));

            var serializerActualType = _specificSerializer.GetType();

            var genericSerializerInterface = serializerActualType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ITypeSerializer<>));

            if (genericSerializerInterface == null)
            {
                throw new ArgumentException($"Provided serializer of type {serializerActualType.FullName} does not implement ITypeSerializer<T>.", nameof(specificSerializerInstance));
            }

            _specificSerializerTypeArgument = genericSerializerInterface.GetGenericArguments()[0];

            _serializeMethod = serializerActualType.GetMethod("Serialize", new[] { _specificSerializerTypeArgument })
                ?? throw new InvalidOperationException($"Serialize method not found on {serializerActualType.FullName} for type {_specificSerializerTypeArgument.FullName}");

            _deserializeMethod = serializerActualType.GetMethod("Deserialize", new[] { typeof(byte[]) })
                ?? throw new InvalidOperationException($"Deserialize method not found on {serializerActualType.FullName}");
        }

        public byte[] Serialize(object obj) {
            ArgumentNullException.ThrowIfNull(obj);
            // Check if obj is an instance of the type the specific serializer expects
            if (!_specificSerializerTypeArgument.IsInstanceOfType(obj))
            {
                throw new ArgumentException($"Object of type {obj.GetType().FullName} is not of expected type {_specificSerializerTypeArgument.FullName}", nameof(obj));
            }

            return (byte[])_serializeMethod.Invoke(_specificSerializer, new[] { obj })!;
        }

        public object Deserialize(byte[] bytes) => _deserializeMethod.Invoke(_specificSerializer, new object[] { bytes })!;
    }
}
