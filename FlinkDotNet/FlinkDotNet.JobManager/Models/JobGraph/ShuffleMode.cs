#nullable enable

namespace FlinkDotNet.JobManager.Models.JobGraph
{
    public enum ShuffleMode
    {
        Forward,    // Pointwise connection (output of subtask i goes to input of subtask i)
        Broadcast,  // Output of each subtask goes to all subtasks of the downstream operator
        Rescale,    // Round-robin or similar for rescaling parallelism (e.g. from 2 to 4, or 4 to 2)
        Hash        // Keyed distribution (records with the same key hash go to the same subtask)
        // Add more as needed (e.g., RangePartition)
    }
}
#nullable disable
