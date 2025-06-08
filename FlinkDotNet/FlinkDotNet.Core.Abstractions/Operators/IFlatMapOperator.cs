using FlinkDotNet.Core.Abstractions.Collectors; // Added using for ICollector

namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Interface for a flatmap operator that transforms one element into zero, one, or more output elements.
    /// Output elements are emitted via an <see cref="ICollector{TOut}"/>.
    /// Similar to Flinks FlatMapFunction.
    /// </summary>
    /// <typeparam name="TIn">The type of the input elements.</typeparam>
    /// <typeparam name="TOut">The type of the output elements.</typeparam>
    public interface IFlatMapOperator<in TIn, out TOut> // Changed TOut to be invariant
    {
        /// <summary>
        /// Transforms the input element into zero, one, or more output elements,
        /// emitting them to the provided collector.
        /// </summary>
        /// <param name="element">The input element.</param>
        /// <param name="collector">The collector to which output elements are emitted.</param>
        void FlatMap(TIn element, ICollector<TOut> collector);
    }
}
