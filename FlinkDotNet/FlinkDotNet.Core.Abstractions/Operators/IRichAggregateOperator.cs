using System.Diagnostics.CodeAnalysis;

namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// A "rich" version of <see cref="IAggregateOperator{TIn, TAgg, TOut}"/> that provides
    /// access to lifecycle methods and runtime context.
    /// Similar to Flinks RichAggregateFunction.
    /// 
    /// Note: Uses contravariant 'in' parameters and covariant 'out' parameter
    /// for proper type variance in functional composition scenarios.
    /// </summary>
    /// <typeparam name="TIn">The type of the input elements.</typeparam>
    /// <typeparam name="TAgg">The type of the accumulator.</typeparam>
    /// <typeparam name="TOut">The type of the output (result) elements.</typeparam>
    [SuppressMessage("Design", "S2436:Reduce the number of generic parameters", 
        Justification = "Three generic parameters are required for proper aggregation function type safety: input type, accumulator type, and output type. This matches Flink's RichAggregateFunction design pattern.")]
    public interface IRichAggregateOperator<in TIn, TAgg, out TOut> : IAggregateOperator<TIn, TAgg, TOut>, IOperatorLifecycle
    {
    }
}
