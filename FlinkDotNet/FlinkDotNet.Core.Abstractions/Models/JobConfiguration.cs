using System.Collections.Generic;

namespace FlinkDotNet.Core.Abstractions.Models
{
    /// <summary>
    /// Represents the configuration parameters for a job.
    /// This is a placeholder and can be expanded to include specific
    /// job-level settings, user parameters, etc.
    /// Flinks equivalent is org.apache.flink.api.common.ExecutionConfig
    /// and the Configuration object passed to RichFunctions.
    /// </summary>
    public static class JobConfiguration
    {
        // For now, keeping it simple as a placeholder.
        // Actual properties will be determined by what global configurations are needed.
        public static string? GetString(string key, string? defaultValue) => defaultValue; // Example method
        public static int GetInt(string key, int defaultValue) => defaultValue; // Example method
    }
}
