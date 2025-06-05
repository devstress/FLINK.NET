namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// A "rich" version of <see cref="IFilterOperator{T}"/> that provides
    /// access to lifecycle methods and runtime context.
    /// Similar to Flinks RichFilterFunction.
    /// </summary>
    /// <typeparam name="T">The type of the elements.</typeparam>
    public interface IRichFilterOperator<in T> : IFilterOperator<T>, IOperatorLifecycle
    {
    }
}
