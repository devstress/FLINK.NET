namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Interface for a reduce operator that combines two elements of the same type
    /// into a single element of that type. This is typically used on keyed streams.
    /// Similar to Flinks ReduceFunction.
    /// </summary>
    /// <typeparam name="T">The type of the elements to be reduced.</typeparam>
    public interface IReduceOperator<T>
    {
        /// <summary>
        /// Combines the current accumulator with a new value to produce a new accumulator.
        /// </summary>
        /// <param name="currentAccumulator">The current accumulated value.</param>
        /// <param name="newValue">The new value to combine with the accumulator.</param>
        /// <returns>The new accumulator value.</returns>
        T Reduce(T currentAccumulator, T newValue);
    }
}
