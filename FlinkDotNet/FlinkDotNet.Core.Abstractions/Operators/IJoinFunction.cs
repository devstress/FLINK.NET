using System.Diagnostics.CodeAnalysis;

namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Interface for a join function that combines two elements from different inputs
    /// into a single output element.
    /// Similar to Flinks JoinFunction.
    /// 
    /// Note: Uses contravariant 'in' parameters and covariant 'out' parameter
    /// for proper type variance in functional composition scenarios.
    /// </summary>
    /// <typeparam name="TLeft">The type of the elements from the left input stream.</typeparam>
    /// <typeparam name="TRight">The type of the elements from the right input stream.</typeparam>
    /// <typeparam name="TOut">The type of the output elements produced by the join.</typeparam>
    [SuppressMessage("Design", "S2436:Reduce the number of generic parameters", 
        Justification = "Three generic parameters are required for proper join function type safety: left input type, right input type, and output type. This matches Flink's JoinFunction design pattern.")]
    public interface IJoinFunction<in TLeft, in TRight, out TOut>
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
