namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// A "rich" version of <see cref="IJoinFunction{TLeft, TRight, TOut}"/> that provides
    /// access to lifecycle methods and runtime context.
    /// Similar to Flinks RichJoinFunction.
    /// </summary>
    /// <typeparam name="TLeft">The type of the elements from the left input stream.</typeparam>
    /// <typeparam name="TRight">The type of the elements from the right input stream.</typeparam>
    /// <typeparam name="TOut">The type of the output elements produced by the join.</typeparam>
    // S2436 (Too many generic parameters) is suppressed. The design with 3 generic parameters is intentional for this operator's clarity.
#pragma warning disable S2436
    public interface IRichJoinFunction<in TLeft, in TRight, out TOut> : IJoinFunction<TLeft, TRight, TOut>, IOperatorLifecycle
#pragma warning restore S2436
    {
    }
}
