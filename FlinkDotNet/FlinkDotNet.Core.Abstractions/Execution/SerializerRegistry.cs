// In FlinkDotNet.Core.Abstractions.Execution.SerializerRegistry.cs
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using FlinkDotNet.Core.Abstractions.Serializers; // Required for ITypeSerializer
using System.Reflection; // Required for GetCustomAttribute
using MemoryPack; // Required for MemoryPackableAttribute

namespace FlinkDotNet.Core.Abstractions.Execution
{
    public class SerializerRegistry
    {
        private readonly ConcurrentDictionary<Type, Type> _typeToSerializerType = new();
        private readonly ConcurrentDictionary<Type, ITypeSerializer> _serializerInstances = new(); // Cache for instances

        // Pre-register basic serializers
        public SerializerRegistry()
        {
            // Assuming BasicSerializers provides a static method to get all basic serializer types
            // or individual registrations. For PoC, let's imagine explicit registration:
            RegisterSerializer(typeof(string), typeof(StringSerializer));
            RegisterSerializer(typeof(int), typeof(IntSerializer));
            RegisterSerializer(typeof(long), typeof(LongSerializer));
            RegisterSerializer(typeof(bool), typeof(BoolSerializer));
            RegisterSerializer(typeof(double), typeof(DoubleSerializer));
            RegisterSerializer(typeof(byte[]), typeof(ByteArraySerializer));
            // Add other basic types as needed from BasicSerializers.cs
        }

        public void RegisterSerializer<TData, TSerializer>()
            where TSerializer : ITypeSerializer<TData>, new()
        {
            _typeToSerializerType[typeof(TData)] = typeof(TSerializer);
        }

        public void RegisterSerializer(Type dataType, Type serializerType)
        {
            if (dataType == null) throw new ArgumentNullException(nameof(dataType));
            if (serializerType == null) throw new ArgumentNullException(nameof(serializerType));

            bool implementsInterface = serializerType.GetInterfaces()
                .Any(i => i.IsGenericType &&
                           i.GetGenericTypeDefinition() == typeof(ITypeSerializer<>) &&
                           i.GetGenericArguments()[0] == dataType);

            if (!implementsInterface)
            {
                 // Attempt to check if serializerType is ITypeSerializer<AnyAssignableBase> for dataType
                 // This logic can get complex. For now, require exact match or handle specific cases.
                 var genericSerializerInterface = serializerType.GetInterfaces()
                    .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ITypeSerializer<>));

                 if (genericSerializerInterface != null)
                 {
                    var genericArg = genericSerializerInterface.GetGenericArguments()[0];
                    if (!genericArg.IsAssignableFrom(dataType))
                    {
                        throw new ArgumentException($"Type {serializerType.FullName} implements ITypeSerializer<{genericArg.Name}> which is not assignable from {dataType.FullName}.", nameof(serializerType));
                    }
                    // If it is assignable (e.g. ITypeSerializer<object> for string), allow it.
                 }
                 else if (!typeof(ITypeSerializer).IsAssignableFrom(serializerType)) // Basic non-generic check (should not happen if ITypeSerializer<T> is the norm)
                 {
                     throw new ArgumentException($"Type {serializerType.FullName} does not implement ITypeSerializer.", nameof(serializerType));
                 }
                 // If it got here, it might be a non-generic ITypeSerializer or ITypeSerializer<BaseType>.
                 // This registration path assumes the user knows what they are doing if not an exact ITypeSerializer<dataType>.
            }
            _typeToSerializerType[dataType] = serializerType;
        }

        /// <summary>
        /// Gets an instance of the appropriate serializer for the given data type.
        /// Prioritizes explicitly registered serializers, then MemoryPack for compatible POCOs,
        /// then basic types.
        /// </summary>
        public ITypeSerializer GetSerializer(Type dataType)
        {
            if (dataType == null) throw new ArgumentNullException(nameof(dataType));

            return _serializerInstances.GetOrAdd(dataType, type =>
            {
                // 1. Check explicitly registered serializer types
                if (_typeToSerializerType.TryGetValue(type, out Type? specificSerializerType))
                {
                    try
                    {
                        return (ITypeSerializer)Activator.CreateInstance(specificSerializerType)!;
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Could not create instance of registered serializer type {specificSerializerType.FullName} for data type {type.FullName}. Ensure it has a public parameterless constructor or is registered as an instance.", ex);
                    }
                }

                // 2. Check for MemoryPack compatibility for class types with [MemoryPackable] attribute
                if (type.IsClass && type.GetCustomAttribute<MemoryPackableAttribute>(inherit: false) != null)
                {
                    try
                    {
                        Type genericMemoryPackSerializerType = typeof(MemoryPackSerializer<>).MakeGenericType(type);
                        Console.WriteLine($"[SerializerRegistry] INFO: Using MemoryPackSerializer for [MemoryPackable] type {type.FullName}");
                        return (ITypeSerializer)Activator.CreateInstance(genericMemoryPackSerializerType)!;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[SerializerRegistry] WARNING: MemoryPackSerializer instantiation failed for [MemoryPackable] type {type.FullName}, will proceed to other fallbacks. Error: {ex.Message}");
                        // Fall through to other attempts if attribute is present but instantiation fails for some reason
                    }
                }

                // 3. If dataType is a class (and not [MemoryPackable] or failed above), attempt MemoryPackSerializer (best-effort)
                if (type.IsClass) // This condition now covers non-[MemoryPackable] classes too
                {
                    try
                    {
                        // This attempt is for POCOs that might not be [MemoryPackable] but MemoryPack might handle via reflection.
                        Type genericMemoryPackSerializerType = typeof(MemoryPackSerializer<>).MakeGenericType(type);
                        Console.WriteLine($"[SerializerRegistry] INFO: Attempting MemoryPackSerializer (best-effort) for non-attributed class type {type.FullName}");
                        return (ITypeSerializer)Activator.CreateInstance(genericMemoryPackSerializerType)!;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[SerializerRegistry] INFO: Best-effort MemoryPackSerializer failed for class type {type.FullName}. Error: {ex.Message}. Proceeding to JsonPocoSerializer fallback.");
                        // Fall through to JsonPocoSerializer
                    }
                }

                // 4. If dataType is a class and MemoryPack attempts failed, use JsonPocoSerializer as a fallback
                if (type.IsClass)
                {
                    try
                    {
                        Type genericJsonPocoSerializerType = typeof(JsonPocoSerializer<>).MakeGenericType(type);
                        Console.WriteLine($"[SerializerRegistry] INFO: Using JsonPocoSerializer as fallback for class type {type.FullName}");
                        return (ITypeSerializer)Activator.CreateInstance(genericJsonPocoSerializerType)!;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[SerializerRegistry] WARNING: JsonPocoSerializer instantiation failed for type {type.FullName}. Error: {ex.Message}");
                        // Fall through to final error
                    }
                }

                // 5. If none of the above, throw SerializationException
                throw new SerializationException($"No serializer could be resolved for type {type.FullName}. " +
                                   $"Consider marking POCOs with [MemoryPackable], registering a custom ITypeSerializer, " +
                                   $"or ensuring it's a supported basic type or simple POCO class.");
            });
        }


        public Type? GetSerializerType(Type dataType) // Kept for JobManager, but resolution logic is now in GetSerializer
        {
            if (_typeToSerializerType.TryGetValue(dataType, out Type? serializerType))
            {
                return serializerType;
            }

            if (dataType.IsClass && dataType.GetCustomAttribute<MemoryPackableAttribute>(inherit: false) != null)
            {
                try
                {
                    // Check if MemoryPack can generate for it (conceptual check)
                    // This doesn't guarantee MemoryPackSerializer will succeed without actual instantiation,
                    // but it's the indicator.
                    return typeof(MemoryPackSerializer<>).MakeGenericType(dataType);
                }
                catch
                {
                    // If MakeGenericType fails for some reason (e.g. constraints on MemoryPackSerializer<T> if any)
                    return null;
                }
            }

            // Basic types would have been pre-registered, so if not found above, they are not explicitly handled here
            // unless GetSerializer is called first which populates _typeToSerializerType for MemoryPack ones.
            // This method might need more sophisticated logic if it's the sole source for TaskDeploymentDescriptor types.

            return null; // No specific type found by these rules, caller might use a default or error.
        }

        public IReadOnlyDictionary<string, string> GetNamedRegistrations()
        {
            // This should ideally reflect all known mappings, including those resolved by MemoryPack.
            // However, it currently only reflects explicitly _typeToSerializerType.
            // This might need enhancement if dynamically resolved serializers need to be listed.
            return _typeToSerializerType.ToDictionary(
                kvp => kvp.Key.AssemblyQualifiedName!,
                kvp => kvp.Value.AssemblyQualifiedName!
            );
        }
    }
}
#nullable disable
