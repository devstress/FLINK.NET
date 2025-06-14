using System.Diagnostics.CodeAnalysis;

namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// A "rich" version of <see cref="IJoinFunction{TLeft, TRight, TOut}"/> that provides
    /// access to lifecycle methods and runtime context.
    /// Similar to Flinks RichJoinFunction.
    /// 
    /// Note: Uses contravariant 'in' parameters and covariant 'out' parameter
    /// for proper type variance in functional composition scenarios.
    /// </summary>
    /// <typeparam name="TLeft">The type of the elements from the left input stream.</typeparam>
    /// <typeparam name="TRight">The type of the elements from the right input stream.</typeparam>
    /// <typeparam name="TOut">The type of the output elements produced by the join.</typeparam>
    [SuppressMessage("Design", "S2436:Reduce the number of generic parameters", 
        Justification = "Three generic parameters are required for proper join function type safety: left input type, right input type, and output type. This matches Flink's RichJoinFunction design pattern.")]
    public interface IRichJoinFunction<in TLeft, in TRight, out TOut> : IJoinFunction<TLeft, TRight, TOut>, IOperatorLifecycle
    {
    }
}
