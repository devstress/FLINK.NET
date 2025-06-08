namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Interface for a join function that combines two elements from different inputs
    /// into a single output element.
    /// Similar to Flinks JoinFunction.
    /// </summary>
    /// <typeparam name="TLeft">The type of the elements from the left input stream.</typeparam>
    /// <typeparam name="TRight">The type of the elements from the right input stream.</typeparam>
    /// <typeparam name="TOut">The type of the output elements produced by the join.</typeparam>
    // TODO: SonarCloud S2436 - Consider refactoring to reduce generic parameters if feasible without major API disruption.
#pragma warning disable S2436
    public interface IJoinFunction<in TLeft, in TRight, out TOut>
#pragma warning restore S2436
    {
        /// <summary>
        /// Joins one element from the left input with one element from the right input.
        /// </summary>
        /// <param name="left">The element from the left input.</param>
        /// <param name="right">The element from the right input.</param>
        /// <returns>The joined output element.</returns>
        TOut Join(TLeft left, TRight right);
    }
}
