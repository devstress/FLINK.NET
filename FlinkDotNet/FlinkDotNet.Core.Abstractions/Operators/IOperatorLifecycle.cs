using FlinkDotNet.Core.Abstractions.Context; // Added using for IRuntimeContext

namespace FlinkDotNet.Core.Abstractions.Operators
{
    /// <summary>
    /// Defines lifecycle methods for operators that require initialization, cleanup,
    /// or access to runtime context. This is analogous to Flinks RichFunction features.
    /// </summary>
    public interface IOperatorLifecycle
    {
        /// <summary>
        /// Initialization method for the operator. It is called before the main
        /// processing methods (e.g., Map, Filter, FlatMap).
        /// The <see cref="IRuntimeContext"/> provides access to runtime information and state.
        /// Flinks equivalent is open(Configuration parameters).
        /// </summary>
        /// <param name="context">The runtime context for this operator instance.</param>
        void Open(IRuntimeContext context);

        /// <summary>
        /// Cleanup method for the operator. It is called after all records have been
        /// processed, or when the operator is being shut down.
        /// Flinks equivalent is close().
        /// </summary>
        void Close();
    }
}
