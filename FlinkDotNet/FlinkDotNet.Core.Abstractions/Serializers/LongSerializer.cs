using System;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    public class LongSerializer : ITypeSerializer<long>
    {
        public byte[] Serialize(long obj) => BitConverter.GetBytes(obj);
        public long Deserialize(byte[] bytes) => BitConverter.ToInt64(bytes, 0);
    }
}
