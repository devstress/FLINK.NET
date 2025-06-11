using System;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace FlinkDotNet.Connectors.Sources.Kafka.Deserializers
{
    /// <summary>
    /// String deserializer for Kafka messages
    /// </summary>
    public class StringDeserializer : IDeserializer<string>
    {
        private readonly Encoding _encoding;

        public StringDeserializer(Encoding? encoding = null)
        {
            _encoding = encoding ?? Encoding.UTF8;
        }

        public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
                return string.Empty;

            return _encoding.GetString(data);
        }
    }

    /// <summary>
    /// JSON deserializer for Kafka messages
    /// </summary>
    /// <typeparam name="T">The type to deserialize to</typeparam>
    public class JsonDeserializer<T> : IDeserializer<T>
    {
        private readonly JsonSerializerOptions _options;

        public JsonDeserializer(JsonSerializerOptions? options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
                return default!;

            return JsonSerializer.Deserialize<T>(data, _options)!;
        }
    }

    /// <summary>
    /// Byte array deserializer for Kafka messages
    /// </summary>
    public class ByteArrayDeserializer : IDeserializer<byte[]>
    {
        public byte[] Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
                return Array.Empty<byte>();

            return data.ToArray();
        }
    }

    /// <summary>
    /// Integer deserializer for Kafka messages
    /// </summary>
    public class IntDeserializer : IDeserializer<int>
    {
        public int Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.Length != 4)
                return 0;

            return BitConverter.ToInt32(data);
        }
    }

    /// <summary>
    /// Long deserializer for Kafka messages  
    /// </summary>
    public class LongDeserializer : IDeserializer<long>
    {
        public long Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.Length != 8)
                return 0;

            return BitConverter.ToInt64(data);
        }
    }
}