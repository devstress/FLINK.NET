#nullable enable
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

        public byte[] Serialize(T record)
        {
            if (record == null)
            {
                // MemoryPack might handle nulls, but explicit handling can be clearer
                // or conform to a specific null representation if needed by Flink.NET.
                // MemoryPack.MemoryPackSerializer.Serialize<T>(null) typically works.
                // Let's assume for now an empty array or specific marker for null if that's a Flink.NET convention.
                // However, MemoryPack itself can serialize a null object.
                // If T is a value type, it cannot be null unless T is Nullable<TValue>.
                // For simplicity, let MemoryPack handle it. If record is null, it serializes as such.
            }

            try
            {
                return MemoryPack.MemoryPackSerializer.Serialize(record);
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

        public T Deserialize(byte[] serializedRecord)
        {
            if (serializedRecord == null)
            {
                // Handle null or empty byte array if it represents null according to Flink.NET conventions
                // For now, assume MemoryPack.Deserialize can handle what it produces for nulls.
                // If T is a value type, this path might be problematic if null was serialized.
            }

            // MemoryPack typically deserializes from ReadOnlySpan<byte>
            // If serializedRecord is empty and represents a null object from Serialize,
            // MemoryPack.Deserialize<T>(ReadOnlySpan<byte>.Empty) might throw or return default(T).
            // This needs to be consistent with Serialize(null).
            // MemoryPack typically expects a non-empty span for non-null objects.

            try
            {
                return MemoryPack.MemoryPackSerializer.Deserialize<T>(serializedRecord);
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

        // TODO: Implement other ITypeSerializer methods if they exist (e.g., CreateInstance, Copy, GetLength)
        // For now, assuming ITypeSerializer only has Serialize and Deserialize for this PoC.
        // If other methods are needed, MemoryPack might offer utilities or they'd need careful implementation.
        // For example, a true deep copy with MemoryPack would be Deserialize(Serialize(record)).
    }

    // Custom SerializationException for Flink.NET (if not already defined elsewhere)
    // If it is defined elsewhere (e.g., in Core.Abstractions), this can be removed.
    // For now, adding it here for completeness of the snippet.
    // [Serializable] // Not strictly needed if not crossing AppDomain boundaries or for basic exception
    public class SerializationException : Exception
    {
        public SerializationException() { }
        public SerializationException(string message) : base(message) { }
        public SerializationException(string message, Exception inner) : base(message, inner) { }
        // protected SerializationException(
        //   System.Runtime.Serialization.SerializationInfo info,
        //   System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
