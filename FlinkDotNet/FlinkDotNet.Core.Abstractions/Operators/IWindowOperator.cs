namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Placeholder interface for a window operator.
    /// The specifics of windowing (assigners, triggers, window functions like ProcessWindowFunction)
    /// will be detailed later. This serves as a marker for now.
    /// TIn is the type of elements in the stream.
    /// TOut is the type of elements produced by the window operation.
    /// </summary>
    /// <typeparam name="TIn">The input element type.</typeparam>
    /// <typeparam name="TOut">The output element type after window processing.</typeparam>
    public interface IWindowOperator<in TIn, out TOut>
    {
        // Placeholder: Actual methods will depend on the chosen windowing model
        // (e.g., Process(IWindow window, IEnumerable<TIn> input, ICollector<TOut> out))
        // For now, it acts as a marker interface.
    }
}
