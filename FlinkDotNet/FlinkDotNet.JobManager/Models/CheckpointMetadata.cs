using System.Collections.Concurrent; // For ConcurrentDictionary
using System.Collections.Generic; // For List
using System.Linq; // For All
using System; // For Console.WriteLine, DateTime

namespace FlinkDotNet.JobManager.Models
{
    public enum CheckpointStatus
    {
        InProgress,
        Completed,
        Failed,
        Aborted
    }

    // This can be expanded from the proto CheckpointTaskInfo if needed,
    // or can directly use proto messages if preferred for internal storage.
    public class TaskCheckpointInfo
    {
        public string TaskManagerId { get; }
        public string SnapshotHandle { get; private set; } // Path or reference
        public ulong SnapshotSize { get; private set; }
        public ulong DurationMs { get; private set; }
        public CheckpointStatus Status { get; private set; } // Status of this specific task's checkpoint

        public TaskCheckpointInfo(string taskManagerId)
        {
            TaskManagerId = taskManagerId;
            SnapshotHandle = string.Empty;
            Status = CheckpointStatus.InProgress;
        }

        public void Complete(string handle, ulong size = 0, ulong duration = 0)
        {
            SnapshotHandle = handle;
            SnapshotSize = size;
            DurationMs = duration;
            Status = CheckpointStatus.Completed;
        }

        public void Fail()
        {
            Status = CheckpointStatus.Failed;
        }
    }

    public class CheckpointMetadata
    {
        public long CheckpointId { get; }
        public long Timestamp { get; } // Timestamp when checkpoint was triggered
        public CheckpointStatus Status { get; private set; }
        public string JobId { get; }

        // Stores info about each task's participation in this checkpoint
        // Key: TaskManagerId (or a more granular subtask ID later)
        public ConcurrentDictionary<string, TaskCheckpointInfo> TaskSnapshots { get; }

        // TODO: ExternalPath can be used to store the location of a master metadata file for the checkpoint if consolidation occurs. Individual task snapshot locations are in TaskSnapshots.
        public string? ExternalPath { get; private set; }

        public CheckpointMetadata(string jobId, long checkpointId, long timestamp, List<string> expectedTaskIds)
        {
            JobId = jobId;
            CheckpointId = checkpointId;
            Timestamp = timestamp;
            Status = CheckpointStatus.InProgress;
            TaskSnapshots = new ConcurrentDictionary<string, TaskCheckpointInfo>();
            foreach (var taskId in expectedTaskIds)
            {
                TaskSnapshots.TryAdd(taskId, new TaskCheckpointInfo(taskId));
            }
        }

        public void MarkCompleted(string? externalPath = null)
        {
            // Logic to check if all tasks are completed before marking the whole checkpoint completed
            if (TaskSnapshots.Values.All(t => t.Status == CheckpointStatus.Completed))
            {
                Status = CheckpointStatus.Completed;
                ExternalPath = externalPath; // If applicable
                Console.WriteLine($"Checkpoint {CheckpointId} for job {JobId} marked COMPLETED.");
            }
            else
            {
                 Console.WriteLine($"Checkpoint {CheckpointId} for job {JobId} attempted to mark complete, but not all tasks are done.");
                // This might indicate a logic error or a premature call.
                // Could also transition to FAILED if some tasks failed.
            }
        }

        public void MarkFailed()
        {
            Status = CheckpointStatus.Failed;
             Console.WriteLine($"Checkpoint {CheckpointId} for job {JobId} marked FAILED.");
        }

         public void MarkTaskAcknowledged(string taskManagerId, string snapshotHandle, ulong size = 0, ulong duration = 0)
        {
            if (TaskSnapshots.TryGetValue(taskManagerId, out var taskInfo))
            {
                taskInfo.Complete(snapshotHandle, size, duration);
            }
        }

        public bool IsFullyAcknowledged() => TaskSnapshots.Values.All(t => t.Status == CheckpointStatus.Completed);
    }
}
#nullable disable
