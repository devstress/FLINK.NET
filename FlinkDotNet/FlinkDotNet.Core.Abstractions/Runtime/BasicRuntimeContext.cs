using System;
using System.Collections.Concurrent; // For ConcurrentDictionary
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Models;
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.States;
using FlinkDotNet.Core.Abstractions.Storage; // Added

namespace FlinkDotNet.Core.Abstractions.Runtime
{
    /// <summary>
    /// A basic, partial implementation of <see cref="IRuntimeContext"/> primarily for
    /// local testing, illustration, or simple single-node execution environments.
    /// This implementation uses in-memory state.
    /// </summary>
    public class BasicRuntimeContext : IRuntimeContext
    {
        public string JobName { get; }
        public string TaskName { get; }
        public int NumberOfParallelSubtasks { get; }
        public int IndexOfThisSubtask { get; }
        public JobConfiguration JobConfiguration { get; }
        public IStateSnapshotStore StateSnapshotStore => _stateSnapshotStore;

        private object? _currentKey; // Stores the current key for keyed state
        private readonly ConcurrentDictionary<object, ConcurrentDictionary<string, object>> _keyedStates = new(); // Assuming this is how state is managed
        private readonly IStateSnapshotStore _stateSnapshotStore;

        public BasicRuntimeContext(
            IStateSnapshotStore? stateSnapshotStore = null,
            string jobName = "DefaultJob",
            string taskName = "DefaultTask",
            int numberOfParallelSubtasks = 1,
            int indexOfThisSubtask = 0,
            JobConfiguration? jobConfiguration = null)
        {
            JobName = jobName;
            TaskName = taskName;
            NumberOfParallelSubtasks = numberOfParallelSubtasks;
            IndexOfThisSubtask = indexOfThisSubtask;
            _stateSnapshotStore = stateSnapshotStore ?? new InMemoryStateSnapshotStore();
            JobConfiguration = jobConfiguration ?? new JobConfiguration();
            _currentKey = null; // Explicitly initialize
        }

        public object? GetCurrentKey()
        {
            return _currentKey;
        }

        public void SetCurrentKey(object? key)
        {
            // This method is intended to be called by the TaskExecutor.
            // If multiple threads were ever to use the same RuntimeContext instance
            // (generally not the case per operator invocation), this would need thread-safety.
            // However, a RuntimeContext is typically per task instance / per record processing scope.
            _currentKey = key;
        }

        public IValueState<T> GetValueState<T>(ValueStateDescriptor<T> stateDescriptor)
        {
            ArgumentNullException.ThrowIfNull(stateDescriptor);

            if (_currentKey == null)
            {
                throw new InvalidOperationException("Cannot get keyed state if current key is not set. Call SetCurrentKey first.");
            }

            var statesForCurrentKey = _keyedStates.GetOrAdd(_currentKey, _ => new ConcurrentDictionary<string, object>());

            object state = statesForCurrentKey.GetOrAdd(stateDescriptor.Name, _ =>
                new InMemoryValueState<T>(stateDescriptor, this)); // Corrected arguments

            if (state is IValueState<T> typedState)
            {
                return typedState;
            }
            else
            {
                throw new InvalidOperationException(
                    $"State with name ''{stateDescriptor.Name}'' already exists but is not of type IValueState<{typeof(T).Name}>.");
            }
        }

        public IListState<T> GetListState<T>(ListStateDescriptor<T> stateDescriptor)
        {
            ArgumentNullException.ThrowIfNull(stateDescriptor);

            if (_currentKey == null)
            {
                throw new InvalidOperationException("Cannot get keyed state if current key is not set. Call SetCurrentKey first.");
            }

            var statesForCurrentKey = _keyedStates.GetOrAdd(_currentKey, _ => new ConcurrentDictionary<string, object>());

            object state = statesForCurrentKey.GetOrAdd(stateDescriptor.Name, _ =>
                new InMemoryListState<T>(stateDescriptor.ElementSerializer));

            if (state is IListState<T> typedState)
            {
                return typedState;
            }
            else
            {
                throw new InvalidOperationException(
                    $"State with name ''{stateDescriptor.Name}'' already exists but is not of type IListState<{typeof(T).Name}>.");
            }
        }

        public IMapState<TK, TV> GetMapState<TK, TV>(MapStateDescriptor<TK, TV> stateDescriptor) where TK : notnull
        {
            ArgumentNullException.ThrowIfNull(stateDescriptor);

            if (_currentKey == null)
            {
                throw new InvalidOperationException("Cannot get keyed state if current key is not set. Call SetCurrentKey first.");
            }

            var statesForCurrentKey = _keyedStates.GetOrAdd(_currentKey, _ => new ConcurrentDictionary<string, object>());

            object state = statesForCurrentKey.GetOrAdd(stateDescriptor.Name, _ =>
                new InMemoryMapState<TK, TV>(stateDescriptor.KeySerializer, stateDescriptor.ValueSerializer));

            if (state is IMapState<TK, TV> typedState)
            {
                return typedState;
            }
            else
            {
                throw new InvalidOperationException(
                    $"State with name ''{stateDescriptor.Name}'' already exists but is not of type IMapState<{typeof(TK).Name}, {typeof(TV).Name}>.");
            }
        }
    }
}
