namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// A "rich" version of <see cref="IReduceOperator{T}"/> that provides
    /// access to lifecycle methods and runtime context.
    /// Similar to Flinks RichReduceFunction.
    /// </summary>
    /// <typeparam name="T">The type of the elements to be reduced.</typeparam>
    public interface IRichReduceOperator<T> : IReduceOperator<T>, IOperatorLifecycle
    {
    }
}
