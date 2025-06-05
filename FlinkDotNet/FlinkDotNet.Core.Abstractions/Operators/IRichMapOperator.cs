namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// A "rich" version of <see cref="IMapOperator{TIn, TOut}"/> that provides
    /// access to lifecycle methods (<see cref="IOperatorLifecycle.Open"/> and
    /// <see cref="IOperatorLifecycle.Close"/>) and runtime context.
    /// Similar to Flinks RichMapFunction.
    /// </summary>
    /// <typeparam name="TIn">The type of the input elements.</typeparam>
    /// <typeparam name="TOut">The type of the output elements.</typeparam>
    public interface IRichMapOperator<in TIn, out TOut> : IMapOperator<TIn, TOut>, IOperatorLifecycle
    {
        // This interface inherits all methods from IMapOperator<TIn, TOut> (i.e., Map)
        // and IOperatorLifecycle (i.e., Open, Close).
        // No additional methods are needed here for now.
    }
}
