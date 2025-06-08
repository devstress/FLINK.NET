using System;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    public class BoolSerializer : ITypeSerializer<bool>
    {
        public byte[] Serialize(bool obj) => BitConverter.GetBytes(obj);
        public bool Deserialize(byte[] bytes) => BitConverter.ToBoolean(bytes, 0);
    }
}
