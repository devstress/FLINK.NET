using System.Diagnostics.CodeAnalysis;

namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Interface for an aggregate operator that computes an aggregate over elements.
    /// It involves creating an accumulator, adding elements to it, and then deriving a final result.
    /// Similar to Flink's AggregateFunction.
    /// 
    /// Note: Uses contravariant 'in' parameters and covariant 'out' parameter
    /// for proper type variance in functional composition scenarios.
    /// </summary>
    /// <typeparam name="TIn">The type of the input elements.</typeparam>
    /// <typeparam name="TAgg">The type of the accumulator.</typeparam>
    /// <typeparam name="TOut">The type of the output (result) elements.</typeparam>
    [SuppressMessage("Design", "S2436:Reduce the number of generic parameters", 
        Justification = "Three generic parameters are required for proper aggregation function type safety: input type, accumulator type, and output type. This matches Apache Flink's AggregateFunction design pattern.")]
    public interface IAggregateOperator<in TIn, TAgg, out TOut>
    {
        /// <summary>
        /// Creates a new accumulator, starting a new aggregate.
        /// </summary>
        /// <returns>A new accumulator.</returns>
        TAgg CreateAccumulator();

        /// <summary>
        /// Adds the given input value to the given accumulator, returning the new accumulator value.
        /// </summary>
        /// <param name="accumulator">The current accumulator.</param>
        /// <param name="value">The value to add.</param>
        /// <returns>The new accumulator.</returns>
        TAgg Add(TAgg accumulator, TIn value);

        /// <summary>
        /// Gets the result of the aggregation from the accumulator.
        /// </summary>
        /// <param name="accumulator">The accumulator.</param>
        /// <returns>The final aggregation result.</returns>
        TOut GetResult(TAgg accumulator);

        /// <summary>
        /// Merges two accumulators, returning an accumulator with the merged state.
        /// This is important for combining intermediate results in distributed computations
        /// or for merging session windows.
        /// </summary>
        /// <param name="a">An accumulator to merge.</param>
        /// <param name="b">Another accumulator to merge.</param>
        /// <returns>The merged accumulator.</returns>
        TAgg Merge(TAgg a, TAgg b);
    }
}
