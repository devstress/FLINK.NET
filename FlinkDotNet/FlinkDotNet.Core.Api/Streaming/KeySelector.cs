namespace FlinkDotNet.Core.Api.Streaming
{
    /// <summary>
    /// Defines a function that extracts a key from an element.
    /// </summary>
    /// <typeparam name="TElement">The type of the element.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="element">The element to extract the key from.</param>
    /// <returns>The extracted key.</returns>
    public delegate TKey KeySelector<in TElement, out TKey>(TElement element);
}
