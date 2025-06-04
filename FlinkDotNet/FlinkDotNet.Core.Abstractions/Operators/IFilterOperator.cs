namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Interface for a filter operator that decides whether to keep an element.
    /// Similar to Flinks FilterFunction.
    /// </summary>
    /// <typeparam name="T">The type of the elements.</typeparam>
    public interface IFilterOperator<in T>
    {
        /// <summary>
        /// Evaluates a condition on the input element.
        /// </summary>
        /// <param name="element">The input element.</param>
        /// <returns>True if the element should be kept, false otherwise.</returns>
        bool Filter(T element);
    }
}
