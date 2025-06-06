#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels; // For Channel as a signaling mechanism
using FlinkDotNet.Core.Abstractions.Models.Checkpointing;

namespace FlinkDotNet.TaskManager
{
    /// <summary>
    /// Represents a request to inject a checkpoint barrier.
    /// </summary>
    public class BarrierInjectionRequest
    {
        public long CheckpointId { get; }
        public long CheckpointTimestamp { get; } // Corrected: Was Timestamp, should be CheckpointTimestamp to match usage

        public BarrierInjectionRequest(long checkpointId, long checkpointTimestamp)
        {
            CheckpointId = checkpointId;
            CheckpointTimestamp = checkpointTimestamp; // Corrected: Was Timestamp
        }
    }

    /// <summary>
    /// Interface for a component that can be signaled to inject a checkpoint barrier.
    /// </summary>
    public interface IBarrierInjectableSource
    {
        string JobId { get; }
        string JobVertexId { get; }
        int SubtaskIndex { get; }
        ChannelWriter<BarrierInjectionRequest> BarrierChannelWriter { get; }
        void ReportCompleted(); // To notify the registry when the source is done
    }

    public class ActiveTaskRegistry
    {
        // Key: $"{JobId}_{JobVertexId}_{SubtaskIndex}"
        private readonly ConcurrentDictionary<string, IBarrierInjectableSource> _activeSources = new();

        public void RegisterSource(IBarrierInjectableSource source)
        {
            var key = GetKey(source.JobId, source.JobVertexId, source.SubtaskIndex);
            _activeSources.TryAdd(key, source);
            Console.WriteLine($"ActiveTaskRegistry: Registered source {key}");
        }

        public void UnregisterSource(string jobId, string jobVertexId, int subtaskIndex)
        {
            var key = GetKey(jobId, jobVertexId, subtaskIndex);
            if (_activeSources.TryRemove(key, out var source))
            {
                // Optionally, ensure the channel is completed if the source didn't do it.
                // source.BarrierChannelWriter.TryComplete();
                Console.WriteLine($"ActiveTaskRegistry: Unregistered source {key}");
            }
        }

        public IEnumerable<IBarrierInjectableSource> GetSourcesForJob(string jobId)
        {
            return _activeSources.Values.Where(s => s.JobId == jobId).ToList();
        }

        private static string GetKey(string jobId, string jobVertexId, int subtaskIndex)
        {
            return $"{jobId}_{jobVertexId}_{subtaskIndex}";
        }
    }
}
#nullable disable
