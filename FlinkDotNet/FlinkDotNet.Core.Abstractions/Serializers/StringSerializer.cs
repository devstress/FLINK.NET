using System.Text;

namespace FlinkDotNet.Core.Abstractions.Serializers
{
    public class StringSerializer : ITypeSerializer<string>
    {
        public byte[] Serialize(string obj) => Encoding.UTF8.GetBytes(obj);
        public string Deserialize(byte[] bytes) => Encoding.UTF8.GetString(bytes);
    }
}
