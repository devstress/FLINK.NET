// In FlinkDotNet.Core.Abstractions.Execution.SerializerRegistry.cs
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using FlinkDotNet.Core.Abstractions.Serializers;
using System.Reflection;
using MemoryPack;

namespace FlinkDotNet.Core.Abstractions.Execution
{
    public class SerializerRegistry
    {
        private readonly ConcurrentDictionary<Type, Type> _typeToSerializerTypeMap = new();
        private readonly ConcurrentDictionary<Type, ITypeSerializer<object>> _serializerInstanceCache = new();

        public SerializerRegistry()
        {
            RegisterSerializer<string, StringSerializer>();
            RegisterSerializer<int, IntSerializer>();
            RegisterSerializer<long, LongSerializer>();
            RegisterSerializer<bool, BoolSerializer>();
            RegisterSerializer<double, DoubleSerializer>();
            RegisterSerializer<byte[], ByteArraySerializer>();
        }

        public void RegisterSerializer<TData, TSerializer>()
            where TSerializer : ITypeSerializer<TData>, new()
        {
            _typeToSerializerTypeMap[typeof(TData)] = typeof(TSerializer);
            _serializerInstanceCache.TryRemove(typeof(TData), out _);
        }

        public void RegisterSerializer(Type dataType, Type serializerType)
        {
            ArgumentNullException.ThrowIfNull(dataType);
            ArgumentNullException.ThrowIfNull(serializerType);

            var genericInterface = serializerType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ITypeSerializer<>));

            if (genericInterface == null)
            {
                throw new ArgumentException($"Type {serializerType.FullName} does not implement ITypeSerializer<T>.", nameof(serializerType));
            }

            var serializerDataType = genericInterface.GetGenericArguments()[0];
            if (!serializerDataType.IsAssignableFrom(dataType) && !dataType.IsAssignableFrom(serializerDataType))
            {
                 throw new ArgumentException($"Serializer type {serializerType.FullName} (ITypeSerializer<{serializerDataType.Name}>) is not assignable to/from data type {dataType.FullName}.", nameof(serializerType));
            }

            _typeToSerializerTypeMap[dataType] = serializerType;
            _serializerInstanceCache.TryRemove(dataType, out _);
        }

        public ITypeSerializer<object> GetSerializer(Type dataType)
        {
            ArgumentNullException.ThrowIfNull(dataType);

            return _serializerInstanceCache.GetOrAdd(dataType, type =>
            {
                if (_typeToSerializerTypeMap.TryGetValue(type, out Type? specificSerializerType))
                {
                    var specificInstance = Activator.CreateInstance(specificSerializerType)!;
                    return new ErasedTypeSerializer(specificInstance); // Pass only instance
                }

                if (type.IsClass && (!type.IsAbstract && !type.IsInterface)) {
                     // Try MemoryPack for known [MemoryPackable] or any non-abstract class as a general POCO serializer
                    if (type.GetCustomAttribute<MemoryPackableAttribute>(inherit: false) != null)
                    {
                        try {
                            var memPackType = typeof(MemoryPackSerializer<>).MakeGenericType(type);
                            var instance = Activator.CreateInstance(memPackType)!;
                            return new ErasedTypeSerializer(instance);
                        } catch { /* fall through to JSON */ }
                    }
                    // Fallback to JSON for other classes if MemoryPack fails or is not applicable
                    try {
                        var jsonType = typeof(JsonPocoSerializer<>).MakeGenericType(type);
                        var instance = Activator.CreateInstance(jsonType)!;
                        return new ErasedTypeSerializer(instance);
                    } catch { /* fall through to exception */ }
                }
                throw new SerializationException($"No serializer could be resolved for type {type.FullName}. " +
                                   "Consider marking POCOs with [MemoryPackable], registering a custom ITypeSerializer, " +
                                   "or ensuring it's a supported basic type or simple POCO class.");
            });
        }
         public Type? GetSerializerType(Type dataType) => _typeToSerializerTypeMap.GetValueOrDefault(dataType);

        public IReadOnlyDictionary<string, string> GetNamedRegistrations()
        {
            return _typeToSerializerTypeMap.ToDictionary(
                kvp => kvp.Key.AssemblyQualifiedName!,
                kvp => kvp.Value.AssemblyQualifiedName!
            );
        }
    }
}
