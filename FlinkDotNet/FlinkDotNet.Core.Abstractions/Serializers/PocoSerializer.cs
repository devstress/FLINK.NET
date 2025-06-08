using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    /// <summary>
    /// DO NOT USE. This serializer uses BinaryFormatter which has security vulnerabilities and poor performance.
    /// It is retained for a transition period only.
    /// Use JsonPocoSerializer<T> as a general default or implement a custom ITypeSerializer<T>
    /// for your types, preferably using System.Text.Json source generation or binary formats like Protobuf/Avro.
    /// </summary>
    [Obsolete("PocoSerializer<T> using BinaryFormatter is deprecated due to security risks and poor performance. Use JsonPocoSerializer<T> or a custom registered serializer instead. This class will be removed in a future version.", error: false)]
    public class PocoSerializer<T> : ITypeSerializer<T>
    {
        public byte[] Serialize(T obj)
        {
            if (obj == null)
            {
                return Array.Empty<byte>();
            }
#pragma warning disable SYSLIB0011 // Type or member is obsolete (suppress for the obsolete class itself)
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
#pragma warning restore SYSLIB0011
        }

        public T Deserialize(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
            {
                return default!;
            }
#pragma warning disable SYSLIB0011 // Type or member is obsolete (suppress for the obsolete class itself)
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream(bytes))
            {
                object obj = bf.Deserialize(ms);
                return (T)obj;
            }
#pragma warning restore SYSLIB0011
        }
    }
}
