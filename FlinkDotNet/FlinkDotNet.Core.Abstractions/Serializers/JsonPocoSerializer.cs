// In FlinkDotNet.Core.Abstractions.Serializers JsonPocoSerializer.cs
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    /// <summary>
    /// A generic serializer for Plain Old CLR Objects (POCOs) using System.Text.Json.
    /// This is intended to be a safer and more performant default replacement
    /// for the old BinaryFormatter-based PocoSerializer.
    /// </summary>
    /// <typeparam name="T">The type of the object to serialize/deserialize.</typeparam>
    public class JsonPocoSerializer<T> : ITypeSerializer<T>
    {
        private static readonly JsonSerializerOptions _defaultOptions;

        static JsonPocoSerializer()
        {
            _defaultOptions = new JsonSerializerOptions
            {
                IncludeFields = true,
                PropertyNameCaseInsensitive = false,
                PropertyNamingPolicy = null,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            };
        }

        public byte[] Serialize(T obj)
        {
            return JsonSerializer.SerializeToUtf8Bytes(obj, _defaultOptions);
        }

        public T Deserialize(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
            {
                // Aligning with original PocoSerializer which returned default! for empty/null bytes.
                // Valid JSON 'null' would be handled by JsonSerializer.Deserialize.
                // Empty byte array is not valid JSON.
                return default!;
            }

            try
            {
                T? result = JsonSerializer.Deserialize<T>(bytes, _defaultOptions);
                return result!;
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"[JsonPocoSerializer] Error deserializing type {typeof(T).FullName}. Bytes length: {bytes.Length}. Error: {ex.Message}");
                // Optionally log part of the bytes if small and safe for logging.
                // For example: Console.WriteLine($"Bytes (first 50): {System.Text.Encoding.UTF8.GetString(bytes, 0, Math.Min(50, bytes.Length))}");
                throw;
            }
        }
    }
}
#nullable disable
