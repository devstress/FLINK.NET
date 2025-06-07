namespace FlinkDotNet.Core.Abstractions.Functions
{
    /// <summary>
    /// Interface for user-defined key selectors.
    /// Allows users to implement custom key extraction logic that can be instantiated by type name.
    /// </summary>
    /// <typeparam name="TIn">The type of the input elements.</typeparam>
    /// <typeparam name="TKey">The type of the extracted key.</typeparam>
    public interface IKeySelector<in TIn, out TKey>
    {
        TKey GetKey(TIn element);
    }
}
#nullable disable
