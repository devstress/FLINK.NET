namespace FlinkDotNet.Core.Abstractions.Context
{
    /// <summary>
    /// Provides information about the context in which an operator is executed.
    /// This includes information about the job, the task, parallelism, and access to state.
    /// This is a placeholder for now and will be expanded significantly.
    /// Similar to Flinks org.apache.flink.api.common.functions.RuntimeContext.
    /// </summary>
    public interface IRuntimeContext
    {
        // --- Placeholder properties/methods - to be defined in detail later ---

        // string JobName { get; }
        // string TaskName { get; }
        // int NumberOfParallelSubtasks { get; }
        // int IndexOfThisSubtask { get; }

        // --- Methods for state access will also go here, e.g. ---
        // IValueState<T> GetValueState<T>(ValueStateDescriptor<T> stateDescriptor);
        // IListState<T> GetListState<T>(ListStateDescriptor<T> stateDescriptor);
        // IMapState<TK, TV> GetMapState<TK, TV>(MapStateDescriptor<TK, TV> stateDescriptor);

        // Other utilities like accumulators, broadcast variables, side outputs etc.
    }
}
