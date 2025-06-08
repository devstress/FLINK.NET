namespace FlinkDotNet.Core.Abstractions.Serializers
{
    public interface ITypeSerializer<T>
    {
        byte[] Serialize(T obj);
        T? Deserialize(byte[] bytes);
    }
}
