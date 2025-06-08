using System;
using System.IO; // For Stream, MemoryStream, although MemoryPack works with ReadOnlySpan<byte>
using MemoryPack; // Main MemoryPack namespace
using FlinkDotNet.Core.Abstractions.Serializers; // Assuming ITypeSerializer is here

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    /// <summary>
    /// An implementation of <see cref="ITypeSerializer{T}"/> that uses MemoryPack
    /// for high-performance binary serialization.
    /// Types to be serialized by this serializer typically need to be annotated with
    /// [MemoryPackable] and relevant MemoryPack attributes for members.
    /// </summary>
    /// <typeparam name="T">The type to be serialized.</typeparam>
    public class MemoryPackSerializer<T> : ITypeSerializer<T>
    {
        // MemoryPack serializers are typically stateless, so a shared instance could be possible
        // if ITypeSerializer instances are managed that way by SerializerRegistry.
        // For now, assume it's instantiated per type as needed.

        public byte[] Serialize(T obj)
        {
            try
            {
                return MemoryPack.MemoryPackSerializer.Serialize(obj);
            }
            catch (MemoryPackSerializationException mpex)
            {
                // Wrap MemoryPack specific exceptions in a Flink.NET serialization exception
                throw new SerializationException($"MemoryPack serialization failed for type {typeof(T).FullName}: {mpex.Message}", mpex);
            }
            catch (Exception ex)
            {
                // Catch other potential exceptions during serialization
                throw new SerializationException($"An unexpected error occurred during MemoryPack serialization for type {typeof(T).FullName}: {ex.Message}", ex);
            }
        }

        public T? Deserialize(byte[] bytes)
        {
            if (bytes == null)
            {
                // Handle null or empty byte array if it represents null according to Flink.NET conventions
                // For now, assume MemoryPack.Deserialize can handle what it produces for nulls.
                // If T is a value type, this path might be problematic if null was serialized.
            }

            // MemoryPack typically deserializes from ReadOnlySpan<byte>
            // If bytes is empty and represents a null object from Serialize,
            // MemoryPack.Deserialize<T>(ReadOnlySpan<byte>.Empty) might throw or return default(T).
            // This needs to be consistent with Serialize(null).
            // MemoryPack typically expects a non-empty span for non-null objects.

            try
            {
                return MemoryPack.MemoryPackSerializer.Deserialize<T>(bytes);
            }
            catch (MemoryPackSerializationException mpex)
            {
                // Wrap MemoryPack specific exceptions
                throw new SerializationException($"MemoryPack deserialization failed for type {typeof(T).FullName}: {mpex.Message}", mpex);
            }
            catch (Exception ex)
            {
                // Catch other potential exceptions
                throw new SerializationException($"An unexpected error occurred during MemoryPack deserialization for type {typeof(T).FullName}: {ex.Message}", ex);
            }
        }
    }
}
