using System;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    public class DoubleSerializer : ITypeSerializer<double>
    {
        public byte[] Serialize(double obj) => BitConverter.GetBytes(obj);
        public double Deserialize(byte[] bytes) => BitConverter.ToDouble(bytes, 0);
    }
}
