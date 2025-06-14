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
    public interface IRichAggregateOperator<in TIn, TAgg, out TOut> : IAggregateOperator<TIn, TAgg, TOut>, IOperatorLifecycle
    {
    }
}
