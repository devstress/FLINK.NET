using FlinkDotNet.Core.Abstractions.Context; // For IRuntimeContext
using System.Threading.Tasks; // For async Open/Close/Invoke if needed

namespace FlinkDotNet.Core.Abstractions.Sinks
{
    /// <summary>
    /// Interface for all Flink data sinks.
    /// Sinks consume records and typically write them to an external system or output.
    /// Sinks must be serializable if they are part of a JobGraph.
    /// </summary>
    /// <typeparam name="TIn">The type of the records consumed by the sink.</typeparam>
    public interface ISinkFunction<TIn>
    {
        /// <summary>
        /// Called once when the sink is initialized.
        /// This can be used to set up connections or resources.
        /// Similar to <see cref="FlinkDotNet.Core.Abstractions.Operators.IOperatorLifecycle.Open"/>.
        /// </summary>
        /// <param name="context">The runtime context of the sink operator.</param>
        void Open(IRuntimeContext context); // Consider Task OpenAsync for async setup

        /// <summary>
        /// This is the main method that is called for each record.
        /// </summary>
        /// <param name="record">The record to be processed.</param>
        /// <param name="context">The context for the sink invocation.</param>
        void Invoke(TIn record, ISinkContext context); // Consider Task InvokeAsync

        /// <summary>
        /// Called once when the sink is shutting down.
        /// This can be used to flush buffers, close connections, etc.
        /// Similar to <see cref="FlinkDotNet.Core.Abstractions.Operators.IOperatorLifecycle.Close"/>.
        /// </summary>
        void Close(); // Consider Task CloseAsync
    }
}
#nullable disable
