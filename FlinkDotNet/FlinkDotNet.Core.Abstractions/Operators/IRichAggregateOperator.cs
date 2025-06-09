namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// A "rich" version of <see cref="IAggregateOperator{TIn, TAgg, TOut}"/> that provides
    /// access to lifecycle methods and runtime context.
    /// Similar to Flinks RichAggregateFunction.
    /// </summary>
    /// <typeparam name="TIn">The type of the input elements.</typeparam>
    /// <typeparam name="TAgg">The type of the accumulator.</typeparam>
    /// <typeparam name="TOut">The type of the output (result) elements.</typeparam>
    // S2436 (Too many generic parameters) is suppressed. The design with 3 generic parameters is intentional for this operator's clarity.
#pragma warning disable S2436
    public interface IRichAggregateOperator<in TIn, TAgg, out TOut> : IAggregateOperator<TIn, TAgg, TOut>, IOperatorLifecycle
#pragma warning restore S2436
    {
    }
}
