using FlinkDotNet.Core.Abstractions.Models;
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.States;
using FlinkDotNet.Core.Abstractions.Storage; // Add this using

namespace FlinkDotNet.Core.Abstractions.Context
{
    /// <summary>
    /// Provides information about the context in which an operator is executed.
    /// This includes information about the job, the task, parallelism, and access to state.
    /// Similar to Flink's org.apache.flink.api.common.functions.RuntimeContext.
    /// </summary>
    public interface IRuntimeContext
    {
        /// <summary>
        /// Gets the name of the job the operator is part of.
        /// </summary>
        string JobName { get; }

        /// <summary>
        /// Gets the name of the task (operator instance).
        /// </summary>
        string TaskName { get; }

        /// <summary>
        /// Gets the total number of parallel subtasks for the operator.
        /// </summary>
        int NumberOfParallelSubtasks { get; }

        /// <summary>
        /// Gets the index of this parallel subtask (0-based).
        /// </summary>
        int IndexOfThisSubtask { get; }

        /// <summary>
        /// Gets the global job configuration.
        /// </summary>
        JobConfiguration JobConfiguration { get; }

        // --- State Access Methods ---

        /// <summary>
        /// Gets a handle to a <see cref="IValueState{T}"/>, which is scoped to the current key.
        /// </summary>
        /// <typeparam name="T">The type of value in the state.</typeparam>
        /// <param name="stateDescriptor">Descriptor that contains the name and other properties for the state.</param>
        /// <returns>The value state.</returns>
        IValueState<T> GetValueState<T>(ValueStateDescriptor<T> stateDescriptor);

        /// <summary>
        /// Gets a handle to an <see cref="IListState{T}"/>, which is scoped to the current key.
        /// </summary>
        /// <typeparam name="T">The type of elements in the list state.</typeparam>
        /// <param name="stateDescriptor">Descriptor that contains the name and other properties for the state.</param>
        /// <returns>The list state.</returns>
        IListState<T> GetListState<T>(ListStateDescriptor<T> stateDescriptor);

        /// <summary>
        /// Gets a handle to an <see cref="IMapState{TK, TV}"/>, which is scoped to the current key.
        /// </summary>
        /// <typeparam name="TK">The type of keys in the map state. Must be notnull.</typeparam>
        /// <typeparam name="TV">The type of values in the map state.</typeparam>
        /// <param name="stateDescriptor">Descriptor that contains the name and other properties for the state.</param>
        /// <returns>The map state.</returns>
        IMapState<TK, TV> GetMapState<TK, TV>(MapStateDescriptor<TK, TV> stateDescriptor) where TK : notnull; // Added notnull constraint

        // Other utilities like accumulators, broadcast variables, side outputs etc.

        /// <summary>
        /// Gets the current key in a keyed context. Returns null if not in a keyed context or key is null.
        /// Used by state implementations to scope state to the current key.
        /// </summary>
        object? GetCurrentKey();

        /// <summary>
        /// INTERNAL USE: Sets the current key. Called by the TaskExecutor before processing a record in a keyed stream.
        /// </summary>
        void SetCurrentKey(object? key);

        /// <summary>
        /// Gets the state snapshot store for this operator, used during checkpointing.
        /// </summary>
        IStateSnapshotStore StateSnapshotStore { get; } // Add this property
    }
}
