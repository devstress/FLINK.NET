using System.Collections.Concurrent;
using FlinkDotNet.TaskManager.Models;

namespace FlinkDotNet.TaskManager
{
    public class ActiveTaskRegistry
    {
        private readonly ConcurrentDictionary<string, string> _tasks = new();
        public void RegisterTask(object owner, string jobId, string vertexId, int subtaskIndex, string taskName)
        {
            _tasks[$"{vertexId}_{subtaskIndex}"] = taskName;
        }
        public void UnregisterTask(string jobId, string vertexId, int subtaskIndex, string taskName)
        {
            _tasks.TryRemove($"{vertexId}_{subtaskIndex}", out _);
        }
        public IEnumerable<SourceTaskWrapper> GetAllSources() => new List<SourceTaskWrapper>();
    }

    public class SourceTaskWrapper
    {
        public string JobId { get; set; } = string.Empty;
        public string JobVertexId { get; set; } = string.Empty;
        public int SubtaskIndex { get; set; }
        public System.Threading.Channels.ChannelWriter<BarrierInjectionRequest> BarrierChannelWriter { get; set; } = System.Threading.Channels.Channel.CreateUnbounded<BarrierInjectionRequest>().Writer;
    }

    public interface IOperatorBarrierHandler
    {
    }
}
