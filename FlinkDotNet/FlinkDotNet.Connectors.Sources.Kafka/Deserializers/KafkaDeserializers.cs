using System;
using System.Text;

namespace FlinkDotNet.Connectors.Sources.Kafka.Deserializers
{
    /// <summary>
    /// Native Kafka deserializers that work with byte arrays directly.
    /// These replace the Confluent.Kafka deserializers in the native implementation.
    /// </summary>
    public static class KafkaDeserializers
    {
        /// <summary>
        /// UTF-8 string deserializer
        /// </summary>
        public static class Utf8
        {
            public static string Deserialize(ReadOnlySpan<byte> data, bool isNull, object? context)
            {
                if (isNull) return null!;
                return Encoding.UTF8.GetString(data);
            }
        }

        /// <summary>
        /// Byte array deserializer (pass-through)
        /// </summary>
        public static class ByteArray
        {
            public static byte[] Deserialize(ReadOnlySpan<byte> data, bool isNull, object? context)
            {
                if (isNull) return null!;
                return data.ToArray();
            }
        }

        /// <summary>
        /// Integer deserializer
        /// </summary>
        public static class Int32
        {
            public static int Deserialize(ReadOnlySpan<byte> data, bool isNull, object? context)
            {
                if (isNull) return 0;
                if (data.Length != 4) throw new ArgumentException("Invalid data length for Int32");
                return BitConverter.ToInt32(data);
            }
        }

        /// <summary>
        /// Long deserializer
        /// </summary>
        public static class Int64
        {
            public static long Deserialize(ReadOnlySpan<byte> data, bool isNull, object? context)
            {
                if (isNull) return 0;
                if (data.Length != 8) throw new ArgumentException("Invalid data length for Int64");
                return BitConverter.ToInt64(data);
            }
        }
    }
}