namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// A "rich" version of <see cref="IFlatMapOperator{TIn, TOut}"/> that provides
    /// access to lifecycle methods and runtime context.
    /// Similar to Flinks RichFlatMapFunction.
    /// </summary>
    /// <typeparam name="TIn">The type of the input elements.</typeparam>
    /// <typeparam name="TOut">The type of the output elements.</typeparam>
    public interface IRichFlatMapOperator<in TIn, TOut> : IFlatMapOperator<TIn, TOut>, IOperatorLifecycle
    {
    }
}
