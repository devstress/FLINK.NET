using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FlinkDotNet.Core.Abstractions.Execution;
using FlinkDotNet.Core.Abstractions.Storage;
using FlinkDotNet.TaskManager.Services;
using FlinkDotNet.Proto.Internal;

namespace FlinkDotNet.TaskManager
{
    public class TaskExecutor
    {
        private readonly ActiveTaskRegistry _activeTaskRegistry;

        public ActiveTaskRegistry Registry => _activeTaskRegistry;

        public TaskExecutor(
            ActiveTaskRegistry activeTaskRegistry,
            TaskManagerCheckpointingServiceImpl checkpointingService,
            SerializerRegistry serializerRegistry,
            string taskManagerId,
            IStateSnapshotStore stateStore)
        {
            _activeTaskRegistry = activeTaskRegistry;
        }

        public Task ExecuteFromDescriptor(
            TaskDeploymentDescriptor descriptor,
            Dictionary<string, string> operatorProperties,
            CancellationToken cancellationToken)
        {
            Console.WriteLine($"[TaskExecutor] ExecuteFromDescriptor for '{descriptor.TaskName}' is not implemented.");
            return Task.CompletedTask;
        }

        public IOperatorBarrierHandler? GetOperatorBarrierHandler(string jobVertexId, int subtaskIndex) => null;
    }
}
