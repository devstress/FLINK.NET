#nullable enable
using System;
using System.Collections.Concurrent; // For ConcurrentDictionary
using FlinkDotNet.Core.Abstractions.Context;
using FlinkDotNet.Core.Abstractions.Models;
using FlinkDotNet.Core.Abstractions.Models.State;
using FlinkDotNet.Core.Abstractions.States;

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

        private readonly ConcurrentDictionary<string, object> _states = new();

        public BasicRuntimeContext(
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
            JobConfiguration = jobConfiguration ?? new JobConfiguration();
        }

        public IValueState<T> GetValueState<T>(ValueStateDescriptor<T> stateDescriptor)
        {
            if (stateDescriptor == null)
            {
                throw new ArgumentNullException(nameof(stateDescriptor));
            }

            object state = _states.GetOrAdd(stateDescriptor.Name, _ =>
                new InMemoryValueState<T>(stateDescriptor.DefaultValue ?? default!)); // Corrected line

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
            throw new NotImplementedException("ListState not implemented in BasicRuntimeContext yet.");
        }

        public IMapState<TK, TV> GetMapState<TK, TV>(MapStateDescriptor<TK, TV> stateDescriptor)
        {
            throw new NotImplementedException("MapState not implemented in BasicRuntimeContext yet.");
        }
    }
}
#nullable disable
