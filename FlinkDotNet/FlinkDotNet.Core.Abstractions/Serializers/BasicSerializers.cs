#nullable enable
using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    public class IntSerializer : ITypeSerializer<int>
    {
        public byte[] Serialize(int obj) => BitConverter.GetBytes(obj);
        public int Deserialize(byte[] bytes) => BitConverter.ToInt32(bytes, 0);
    }

    public class LongSerializer : ITypeSerializer<long>
    {
        public byte[] Serialize(long obj) => BitConverter.GetBytes(obj);
        public long Deserialize(byte[] bytes) => BitConverter.ToInt64(bytes, 0);
    }

    public class StringSerializer : ITypeSerializer<string>
    {
        public byte[] Serialize(string obj) => Encoding.UTF8.GetBytes(obj);
        public string Deserialize(byte[] bytes) => Encoding.UTF8.GetString(bytes);
    }

    // Note: BinaryFormatter has security vulnerabilities and is not recommended for untrusted data.
    // This is a basic PocoSerializer for demonstration. For production, a more robust serializer like
    // Newtonsoft.Json, System.Text.Json, or Apache Avro specific C# implementation would be preferred.
    public class PocoSerializer<T> : ITypeSerializer<T>
    {
        public byte[] Serialize(T obj)
        {
            if (obj == null) return Array.Empty<byte>();
            #pragma warning disable SYSLIB0011 // Type or member is obsolete
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
            #pragma warning restore SYSLIB0011 // Type or member is obsolete
        }

        public T Deserialize(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0) return default!;
            #pragma warning disable SYSLIB0011 // Type or member is obsolete
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream(bytes))
            {
                object obj = bf.Deserialize(ms);
                return (T)obj;
            }
            #pragma warning restore SYSLIB0011 // Type or member is obsolete
        }
    }
}
#nullable disable
