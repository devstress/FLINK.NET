using System;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    public class IntSerializer : ITypeSerializer<int>
    {
        public byte[] Serialize(int obj) => BitConverter.GetBytes(obj);
        public int Deserialize(byte[] bytes) => BitConverter.ToInt32(bytes, 0);
    }
}
