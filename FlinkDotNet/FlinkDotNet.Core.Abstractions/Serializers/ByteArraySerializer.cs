using System;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    public class ByteArraySerializer : ITypeSerializer<byte[]>
    {
        public byte[] Serialize(byte[] obj) { // Return a copy to prevent modification of original if it's pooled/reused
            if (obj == null)
            {
                return null!; // Or throw ArgumentNullException, or return Array.Empty<byte>() if appropriate
            }
            byte[] copy = new byte[obj.Length];
            Buffer.BlockCopy(obj, 0, copy, 0, obj.Length);
            return copy;
        }
        public byte[] Deserialize(byte[] bytes) { // Return a copy
            if (bytes == null)
            {
                return null!;
            }
            byte[] copy = new byte[bytes.Length];
            Buffer.BlockCopy(bytes, 0, copy, 0, bytes.Length);
            return copy;
        }
    }
}
