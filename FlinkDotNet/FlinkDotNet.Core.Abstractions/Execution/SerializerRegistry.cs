// In FlinkDotNet.Core.Abstractions.Execution.SerializerRegistry.cs
#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using FlinkDotNet.Core.Abstractions.Serializers; // Required for ITypeSerializer

namespace FlinkDotNet.Core.Abstractions.Execution
{
    public class SerializerRegistry
    {
        private readonly ConcurrentDictionary<Type, Type> _typeToSerializerType = new();

        /// <summary>
        /// Registers a specific serializer type for a given data type.
        /// The serializer type must implement ITypeSerializer for the given data type
        /// and have a public parameterless constructor.
        /// </summary>
        /// <typeparam name="TData">The data type for which the serializer is being registered.</typeparam>
        /// <typeparam name="TSerializer">The serializer type, which must implement ITypeSerializer<TData> and have a new().</typeparam>
        public void RegisterSerializer<TData, TSerializer>()
            where TSerializer : ITypeSerializer<TData>, new()
        {
            _typeToSerializerType[typeof(TData)] = typeof(TSerializer);
            // Console.WriteLine($"[SerializerRegistry] Serializer {typeof(TSerializer).FullName} registered for type {typeof(TData).FullName}");
        }

        /// <summary>
        /// Registers a specific serializer type for a given data type.
        /// Use this overload if the serializer does not have a public parameterless constructor
        /// or if type checking needs to be more dynamic. TaskExecutor will use Activator.CreateInstance.
        /// </summary>
        /// <param name="dataType">The data type for which the serializer is being registered.</param>
        /// <param name="serializerType">The serializer type. Must implement ITypeSerializer for the given dataType.</param>
        /// <exception cref="ArgumentNullException">If dataType or serializerType is null.</exception>
        /// <exception cref="ArgumentException">If serializerType does not implement ITypeSerializer or
        /// if it does not implement the correctly typed ITypeSerializer for dataType.</exception>
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
                // Fallback check for cases where dataType might be a derived type of the T in ITypeSerializer<T>
                // e.g. registering ITypeSerializer<object> for type string. This is more complex.
                // For now, demand exact generic match or non-generic ITypeSerializer (though ITypeSerializer<T> is preferred).
                if (!typeof(ITypeSerializer).IsAssignableFrom(serializerType)) // Basic non-generic check
                {
                     throw new ArgumentException($"Type {serializerType.FullName} does not implement ITypeSerializer.", nameof(serializerType));
                }
                 // The more specific check above is better. If it fails, it means it's not ITypeSerializer<dataType>.
                 // A common scenario might be registering ITypeSerializer<object> for any type if no specific one is found.
                 // However, the generic argument must match for type safety with ITypeSerializer<T>.
                 throw new ArgumentException($"Type {serializerType.FullName} does not implement ITypeSerializer<{dataType.FullName}>.", nameof(serializerType));
            }

            // Check if it has a public parameterless constructor if we want to enforce it for Activator.CreateInstance without args.
            // TaskExecutor currently uses Activator.CreateInstance, which can handle constructors with parameters if they are resolvable
            // via DI, but for serializers, parameterless is common. No strict enforcement here, as TaskExecutor will handle instantiation.

            _typeToSerializerType[dataType] = serializerType;
            // Console.WriteLine($"[SerializerRegistry] Serializer {serializerType.FullName} registered for type {dataType.FullName}");
        }

        /// <summary>
        /// Gets the registered serializer type (as a Type) for a given data type.
        /// Used by JobManager to get the fully qualified name for the TaskDeploymentDescriptor.
        /// </summary>
        /// <param name="dataType">The data type.</param>
        /// <returns>The registered serializer type (Type object), or null if no specific serializer is registered.</returns>
        public Type? GetSerializerType(Type dataType)
        {
            _typeToSerializerType.TryGetValue(dataType, out Type? serializerType);
            return serializerType;
        }

        /// <summary>
        /// Gets all serializer registrations as a dictionary of type full names.
        /// This is intended for serialization and transfer to the JobManager.
        /// </summary>
        /// <returns>A read-only dictionary mapping data type assembly-qualified names to serializer type assembly-qualified names.</returns>
        public IReadOnlyDictionary<string, string> GetNamedRegistrations()
        {
            return _typeToSerializerType.ToDictionary(
                kvp => kvp.Key.AssemblyQualifiedName!, // Assumes type has valid AssemblyQualifiedName
                kvp => kvp.Value.AssemblyQualifiedName!
            );
        }
    }
}
#nullable disable
