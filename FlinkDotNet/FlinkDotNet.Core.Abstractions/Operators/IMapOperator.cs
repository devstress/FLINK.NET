namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Interface for a map operator that transforms one element into another.
    /// Similar to Flinks MapFunction.
    /// </summary>
    /// <typeparam name="TIn">The type of the input elements.</typeparam>
    /// <typeparam name="TOut">The type of the output elements.</typeparam>
    public interface IMapOperator<in TIn, out TOut>
    {
        /// <summary>
        /// Transforms the input element into an output element.
        /// </summary>
        /// <param name="element">The input element.</param>
        /// <returns>The transformed output element.</returns>
        TOut Map(TIn element);
    }
}
